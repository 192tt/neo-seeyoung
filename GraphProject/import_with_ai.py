import pandas as pd
import os
from neo4j import GraphDatabase
from tqdm import tqdm
import math
import datetime
import re
import dashscope
from http import HTTPStatus
import time

# ================= 配置 =================
# 阿里云 DashScope 配置
dashscope.api_key = "sk-9e6ab636a87c4596b2cc72f25a52cf5b"
MODEL_NAME = "qwen-plus"  # 使用能力更强的 Plus 模型

URI = "neo4j://127.0.0.1:7687"
AUTH = ("neo4j", "zhihui123")
FILE_NAME = "全部信息_全(1).xlsx"


# ================= AI 辅助函数 =================
def clean_str(val):
    if pd.isna(val) or val is None: return ""
    s = str(val).strip()
    if s.lower() in ['nan', 'none', 'null', '无', '-', '0']: return ""
    return s


def call_qwen_summary(company_name, raw_tech, raw_scope, raw_intro):
    """调用通义千问生成结构化的产品服务总结"""

    # 构建提示词
    content = f"""
    你是一名智慧养老行业的资深分析师。请根据以下企业信息，总结该企业的【核心产品与具体服务】。

    【输入信息】
    企业名称：{company_name}
    核心技术/产品字段：{raw_tech}
    经营范围：{raw_scope}
    简介：{raw_intro}

    【要求】
    1. 输出格式必须是 HTML 的无序列表 (<ul><li>...</li></ul>)。
    2. 提炼出 3-5 个最具代表性的产品或服务项。
    3. 去除废话，语言简练、专业。
    4. 如果信息太少无法总结，请直接返回空字符串。
    5. 不要包含 ```html 这种代码块标记，直接返回 HTML 代码。
    """

    try:
        response = dashscope.Generation.call(
            model=MODEL_NAME,
            messages=[{'role': 'user', 'content': content}],
            result_format='message'
        )
        if response.status_code == HTTPStatus.OK:
            text = response.output.choices[0].message.content
            # 清洗可能存在的 markdown 标记
            text = text.replace('```html', '').replace('```', '').strip()
            return text
        else:
            print(f"⚠️ API 调用失败: {response.code}")
            return ""
    except Exception as e:
        print(f"⚠️ 请求异常: {e}")
        return ""


# ================= 现有辅助函数 =================
def get_town_code_and_name(address_text, company_name):
    VALID_TOWNS = [
        "上地街道", "万寿路街道", "西三旗街道", "中关村街道", "永定路街道",
        "学院路街道", "马连洼街道", "清河街道", "四季青镇", "北太平庄街道",
        "花园路街道", "东升镇", "八里庄街道", "田村路街道", "北下关街道",
        "紫竹院街道", "温泉镇", "上庄镇", "苏家坨镇", "西北旺镇",
        "羊坊店街道", "甘家口街道", "曙光街道", "香山街道", "燕园街道",
        "清华园街道", "海淀街道", "青龙桥街道"
    ]
    TOWN_MAP = {name: f"{i + 1:02d}" for i, name in enumerate(VALID_TOWNS)}
    txt = str(address_text) + str(company_name)
    for town in VALID_TOWNS:
        if town in txt: return town, TOWN_MAP[town]
    if "街道" in txt:
        try:
            idx = txt.find("街道")
            if idx >= 2: return txt[idx - 2:idx + 2], "98"
        except:
            pass
    if "镇" in txt:
        try:
            idx = txt.find("镇")
            if idx >= 2: return txt[idx - 2:idx + 1], "98"
        except:
            pass
    return "未知", "00"


def extract_tags(row):
    tags = []
    zizhi = clean_str(row.get('单位资质'))
    if zizhi: tags.extend([x.strip() for x in re.split(r'[，,；;\n]', zizhi) if x.strip()])
    rongyu = clean_str(row.get('单位荣誉'))
    if rongyu: tags.extend([x.strip() for x in re.split(r'[，,；;\n]', rongyu) if x.strip()])
    return ",".join(list(set(tags))[:8])


def calculate_scores(row):
    # 1. 定义一个内部安全转换函数，专门解决 NaN 转 int 的问题
    def safe_int(val):
        try:
            if pd.isna(val) or val is None:
                return 0
            # 先转 float 处理 "50.0" 这种字符串，再转 int
            f_val = float(val)
            if math.isnan(f_val):
                return 0
            return int(f_val)
        except:
            return 0

    # 2. 计算逻辑 (保持不变)
    def parse_len(text):
        return 0 if pd.isna(text) else len(str(text).split(','))

    tech_score = min(100, parse_len(row.get('单位资质')) * 10)

    cap_val = 0
    try:
        cap_str = str(row.get('注册资本(万)', '0'))
        # 提取数字
        found = re.findall(r"\d+\.?\d*", cap_str)
        if found:
            cap_val = float(found[0])
    except:
        cap_val = 0

    strength_score = min(100, math.log(cap_val + 1) * 10)

    # 这里的 get 如果取到 NaN，上面的 safe_int 会救场
    conf = row.get('置信度', 50)

    total = (safe_int(conf) * 0.4) + (strength_score * 0.3) + (tech_score * 0.3)

    def to_star(val):
        return round(val / 20.0, 1)

    # 3. 返回时全部套用 safe_int
    return (
        safe_int(tech_score),
        safe_int(strength_score),
        safe_int(conf),
        to_star(total),
        to_star(tech_score),
        to_star(strength_score),
        to_star(safe_int(conf)),
        total
    )


# ================= 主逻辑 =================
def import_data():
    if not os.path.exists(FILE_NAME):
        print(f"❌ 错误：找不到文件 {FILE_NAME}")
        return

    print("🚀 读取 Excel 数据...")
    df = pd.read_excel(FILE_NAME)
    df = df[df['是否属于智慧养老'].astype(str).str.contains('1', na=False)]

    # 限制数量用于测试（如果全量跑，请注释掉下面这行）
    # df = df.head(100)

    STREAM_MAP = {"上游": "1", "中游": "2", "下游": "3"}
    data_map = {}

    print("🤖 开始处理数据并调用 Qwen 模型生成摘要 (这可能需要一些时间)...")

    for idx, row in tqdm(df.iterrows(), total=len(df)):
        stream = row.get('上中下游', '未知')
        raw_l3 = str(row.get('细分小类', '其他')).replace("：", ":")
        name = clean_str(row.get('企业名称'))
        if not name: continue

        # 根据原始数据中的前缀设置二级节点
        l2_plate = "未知板块"
        if stream == "上游":
            l2_plate = "技术支撑层"
        elif stream == "中游":
            if '服务:' in raw_l3:
                l2_plate = "智慧服务"
            elif '产品:' in raw_l3:
                l2_plate = "智慧产品"
            else:
                l2_plate = "智慧产品" if '产品' in raw_l3 else "智慧服务"
        elif stream == "下游":
            if '居家养老' in raw_l3:
                l2_plate = "居家养老"
            elif '机构养老' in raw_l3:
                l2_plate = "机构养老"
            elif '社区养老' in raw_l3:
                l2_plate = "社区养老"
            else:
                l2_plate = "养老服务"

        # 提取冒号后面的部分作为三级节点名称
        # 同时处理中文冒号和英文冒号的情况
        if '：' in raw_l3:
            parts = raw_l3.split('：')
        elif ':' in raw_l3:
            parts = raw_l3.split(':')
        else:
            parts = [raw_l3]
        l3_cat = parts[1].strip() if len(parts) > 1 else parts[0].strip()

        # 准备数据
        addr = clean_str(row.get('经营地')) or clean_str(row.get('注册地'))
        town_name, town_code = get_town_code_and_name(addr, name)
        scores = calculate_scores(row)
        tags = extract_tags(row)

        # ★★★ AI 生成部分 ★★★
        # 为了不完全阻塞，只有当存在相关信息时才调用
        raw_tech = clean_str(row.get('核心技术/产品/服务'))
        raw_scope = clean_str(row.get('主营产品/服务'))  # 使用主营产品/服务替代经营范围
        raw_intro = clean_str(row.get('公司简介'))

        # 调用 AI (注意：大量数据会比较慢，请耐心等待)
        # 这里会将 AI 生成的结果存入 product_summary
        ai_summary = ""
        if raw_tech or raw_scope or raw_intro:
            # 简单的限流，防止QPS过高（Plus模型根据你的等级有限制）
            ai_summary = call_qwen_summary(name, raw_tech, raw_scope, raw_intro)

        key = f"{stream}|{l2_plate}|{l3_cat}"
        if key not in data_map: data_map[key] = []

        data_map[key].append({
            'name': name, 'row': row, 'town': town_name, 'town_code': town_code,
            'scores': scores, 'tags': tags, 'address': addr,
            'contact': clean_str(row.get('联系电话')),
            'ai_summary': ai_summary  # 新字段
        })

    # ================= 入库 =================
    print("💾 正在写入 Neo4j...")
    driver = GraphDatabase.driver(URI, auth=AUTH)
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        session.run(
            "MERGE (:Node {id:'上游', name:'上游', level:1}) MERGE (:Node {id:'中游', name:'中游', level:1}) MERGE (:Node {id:'下游', name:'下游', level:1})")
        session.run("MATCH (u:Node{id:'上游'}), (m:Node{id:'中游'}) MERGE (u)-[:LINK]->(m)")
        session.run("MATCH (m:Node{id:'中游'}), (d:Node{id:'下游'}) MERGE (m)-[:LINK]->(d)")

        for key, items in data_map.items():
            stream, plate, l3_cat = key.split('|')
            items.sort(key=lambda x: x['scores'][7], reverse=True)
            l2_id = f"{stream}-{plate}"
            l3_id = f"{stream}-{plate}-{l3_cat}"

            session.run(
                "MATCH (l1:Node {id: $stream}) MERGE (l2:Node {id: $l2_id, name: $plate, level: 2, parent: $stream}) MERGE (l1)-[:LINK]->(l2)",
                stream=stream, l2_id=l2_id, plate=plate)
            session.run(
                "MATCH (l2:Node {id: $l2_id}) MERGE (l3:Node {id: $l3_id, name: $l3_cat, level: 3, parent: $l2_id}) MERGE (l2)-[:LINK]->(l3)",
                l2_id=l2_id, l3_id=l3_id, l3_cat=l3_cat)

            for rank, item in enumerate(items, 1):
                final_code = f"CODE{rank}"  # 简化的编码
                row = item['row']
                s = item['scores']

                session.run("""
                    MATCH (l3:Node {id: $l3_id})
                    MERGE (l4:Node {id: $name, name: $name, level: 4, category: '企业'})
                    SET l4.code=$code, l4.rank=$rank, 
                        l4.star_total=$star_total, l4.star_tech=$star_tech, l4.star_str=$star_str,
                        l4.tech_text=$tech_text, 
                        l4.product_services=$product_services, 
                        l4.scene_text=$scene_text, 
                        l4.intro=$intro, 
                        l4.legal=$legal, l4.capital=$capital, l4.date=$date,
                        l4.contact=$contact, l4.address=$address, l4.tags=$tags,
                        l4.insured=$insured, l4.company_type=$company_type,
                        l4.industry_stream=$industry_stream, l4.sub_category=$sub_category,
                        l4.confidence=$confidence,
                        l4.parent=$l3_id
                    MERGE (l3)-[:HAS_CHILD]->(l4)
                """, l3_id=l3_id, name=item['name'], code=final_code, rank=rank,
                            star_total=s[3], star_tech=s[4], star_str=s[5],
                            tech_text=clean_str(row.get('核心技术/产品/服务')),
                            product_services=item['ai_summary'],  # ★★★ 写入AI生成的内容
                            scene_text=clean_str(row.get('主要应用场景')),
                            intro=clean_str(row.get('公司简介')),
                            legal=clean_str(row.get('法人')),
                            capital=clean_str(row.get('注册资本(万)')),
                            date=clean_str(row.get('成立日期')),
                            contact=item['contact'],
                            address=item['address'],
                            tags=item['tags'],
                            insured=clean_str(row.get('参保人数')),
                            company_type=clean_str(row.get('企业类型')),
                            industry_stream=clean_str(row.get('上中下游')),
                            sub_category=clean_str(row.get('细分小类')),
                            confidence=float(row.get('置信度', 0)) if row.get('置信度') else 0
                            )

                if item['town'] != '未知':
                    session.run(
                        "MATCH (l4:Node {id: $name}) MERGE (l5:Node {id: $town, name: $town, level: 5, category: '街镇'}) MERGE (l4)-[:LOCATED_IN]->(l5)",
                        name=item['name'], town=item['town'])

    driver.close()
    print("✅ 数据重构完成！Qwen 模型已为您生成产品服务摘要。")


if __name__ == "__main__":
    import_data()