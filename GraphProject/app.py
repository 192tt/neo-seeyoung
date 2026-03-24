import math
import os
import uuid
import json
from flask import Flask, render_template, jsonify, request
from neo4j import GraphDatabase
import pandas as pd
# 注意：由于环境限制，我们暂时使用模拟实现
# 实际应用中可以取消注释以下代码并使用真实的API密钥
import dashscope

# 设置阿里云API密钥
dashscope.api_key = "sk-9e6ab636a87c4596b2cc72f25a52cf5b"

app = Flask(__name__)
# URI = "neo4j://127.0.0.1:7687"
# AUTH = ("neo4j", "zhihui123")
URI = "neo4j://127.0.0.1:7687"
AUTH = ("neo4j", "zhihui123")
EXCEL_FILE = os.path.join(os.path.dirname(__file__), "全部信息_全(1).xlsx")
_excel_cache = None


def safe_val(val):
    if val is None: return ""
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)): return 0
    return val


def pick_column(columns, *fragments, default=None):
    for col in columns:
        name = str(col)
        if all(fragment in name for fragment in fragments):
            return col
    return default


def resolve_excel_path():
    candidates = [
        EXCEL_FILE,
        os.path.join(os.getcwd(), "全部信息_全(1).xlsx"),
        os.path.join(os.path.dirname(__file__), "..", "全部信息_全(1).xlsx"),
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return EXCEL_FILE


def load_excel_data():
    global _excel_cache
    if _excel_cache:
        return _excel_cache

    excel_path = resolve_excel_path()
    df = pd.read_excel(excel_path)

    cols = df.columns
    col_name = pick_column(cols, "企", "名") or pick_column(cols, "名称") or cols[0]
    col_stream = pick_column(cols, "上中下游")
    col_subcat = pick_column(cols, "细分") or pick_column(cols, "小类")
    col_intro = pick_column(cols, "简介")
    col_scene = pick_column(cols, "场景")
    col_tech = pick_column(cols, "技术") or pick_column(cols, "产品")
    col_services = pick_column(cols, "服务") or pick_column(cols, "产品")
    col_credit = pick_column(cols, "社会信用") or pick_column(cols, "信用")
    col_tags = pick_column(cols, "经营范围") or pick_column(cols, "标签")
    col_rank = pick_column(cols, "排名") or pick_column(cols, "排序") or pick_column(cols, "rank")
    col_town = pick_column(cols, "街镇") or pick_column(cols, "街道") or pick_column(cols, "乡镇")
    print(f"街镇列: {col_town}")

    nodes = []
    links = []
    root_id = "ROOT"
    nodes.append({
        "id": root_id,
        "name": "产业链图谱",
        "level": 1,
        "category": "Root",
        "parent": None,
        "details": {}
    })

    stream_map = {}
    subcat_map = {}
    town_map = {}
    name_to_node = {}

    for idx, row in df.iterrows():
        name = str(safe_val(row.get(col_name, ""))).strip()
        if not name or name == "0":
            continue

        stream = str(safe_val(row.get(col_stream, "未分层"))) if col_stream else "未分层"
        stream = stream.strip() or "未分层"
        if stream == "0":
            stream = "未分层"
        subcat = str(safe_val(row.get(col_subcat, "未分类"))) if col_subcat else "未分类"
        subcat = subcat.strip() or "未分类"
        if subcat == "0":
            subcat = "未分类"

        if stream not in stream_map:
            stream_id = f"stream_{len(stream_map)}"
            stream_map[stream] = stream_id
            nodes.append({
                "id": stream_id,
                "name": stream,
                "level": 2,
                "category": "Stream",
                "parent": root_id,
                "details": {}
            })
            links.append({"source": stream_id, "target": root_id})

        stream_id = stream_map[stream]

        subcat_key = (stream, subcat)
        if subcat_key not in subcat_map:
            subcat_id = f"subcat_{len(subcat_map)}"
            subcat_map[subcat_key] = subcat_id
            nodes.append({
                "id": subcat_id,
                "name": subcat,
                "level": 3,
                "category": "SubCategory",
                "parent": stream_id,
                "details": {}
            })
            links.append({"source": subcat_id, "target": stream_id})

        subcat_id = subcat_map[subcat_key]

        company_id = f"company_{idx}"
        rank_val = safe_val(row.get(col_rank, idx + 1))
        if not isinstance(rank_val, (int, float)) or math.isnan(rank_val):
            rank_val = idx + 1

        tech_text = safe_val(row.get(col_tech, "-"))
        scene_text = safe_val(row.get(col_scene, "-"))
        product_services = safe_val(row.get(col_services, tech_text))

        details = {
            "code": safe_val(row.get(col_credit, f"C{idx + 1:05d}")),
            "rank": rank_val,
            "total_siblings": 1,
            "stars": 3,
            "star_tech": 3,
            "star_str": 3,
            "star_rel": 3,
            "intro": safe_val(row.get(col_intro, "-")),
            "tech": tech_text,
            "scene": scene_text,
            "legal": safe_val(row.get("法人代表", "-")) if "法人代表" in df.columns else "-",
            "capital": safe_val(row.get("注册资本", "-")) if "注册资本" in df.columns else "-",
            "date": safe_val(row.get("成立日期", "-")) if "成立日期" in df.columns else "-",
            "insured": safe_val(row.get("参保人数", "-")) if "参保人数" in df.columns else "-",
            "company_type": safe_val(row.get("企业类型", "-")) if "企业类型" in df.columns else "-",
            "industry_stream": stream,
            "sub_category": subcat,
            "confidence": "-",
            "contact": safe_val(row.get("联系方式", "-")) if "联系方式" in df.columns else "-",
            "address": safe_val(row.get("地址", "-")) if "地址" in df.columns else "-",
            "tags": safe_val(row.get(col_tags, "-")) if col_tags else "-",
            "product_services": product_services,
            "tech_text": tech_text,
            "scene_text": scene_text
        }

        nodes.append({
            "id": company_id,
            "name": name,
            "level": 4,
            "category": "Company",
            "parent": subcat_id,
            "details": details
        })
        links.append({"source": company_id, "target": subcat_id})
        name_to_node[name] = company_id

        # 处理街镇节点
        town = str(safe_val(row.get(col_town, "未指定街镇"))).strip() if col_town else "未指定街镇"
        if town and town != "0" and town != "未指定街镇":
            if town not in town_map:
                town_id = f"town_{len(town_map)}"
                town_map[town] = town_id
                nodes.append({
                    "id": town_id,
                    "name": town,
                    "level": 5,
                    "category": "Town",
                    "parent": root_id,
                    "details": {}
                })
                links.append({"source": town_id, "target": root_id})
            town_id = town_map[town]
            # 建立企业与街镇的连接
            links.append({"source": company_id, "target": town_id})

    _excel_cache = {
        "df": df,
        "nodes": nodes,
        "links": links,
        "columns": {
            "name": col_name,
            "stream": col_stream,
            "subcat": col_subcat,
            "intro": col_intro,
            "scene": col_scene,
            "tech": col_tech,
            "services": col_services,
            "credit": col_credit,
            "tags": col_tags,
            "rank": col_rank,
            "town": col_town
        },
        "name_to_node": name_to_node,
        "town_map": town_map
    }
    return _excel_cache


def get_data_from_excel():
    data = load_excel_data()
    return {"nodes": data["nodes"], "links": data["links"]}


# Neo4j 数据源已停用，保留原实现以备后续恢复
"""
def get_data_from_neo4j():
    driver = GraphDatabase.driver(URI, auth=AUTH)
    # ...
"""


def get_data_from_neo4j():
    driver = GraphDatabase.driver(URI, auth=AUTH)
    nodes = []
    links = []
    try:
        with driver.session() as session:
            # 先查询所有节点，包括街镇节点
            result = session.run("""
                MATCH (n) 
                RETURN n.id, n.name, n.level, n.category, n.parent, n.rank, n.code,
                       n.star_total, n.star_tech, n.star_str, n.star_rel,
                       n.intro, n.legal, n.capital, n.date, n.tech_text, n.scene_text,
                       n.confidence, n.insured, n.company_type, n.industry_stream, n.sub_category,
                       n.contact, n.address, n.tags, n.product_services
                LIMIT 10000
            """)

            cat_counts = {}
            raw_records = list(result)
            print("查询到的节点数量:", len(raw_records))
            for r in raw_records:
                if r['n.level'] == 4:
                    parent = r['n.parent']
                    cat_counts[parent] = cat_counts.get(parent, 0) + 1

            seen_ids = set()
            town_count = 0
            for record in raw_records:
                nid = record["n.id"]
                if nid in seen_ids: continue
                seen_ids.add(nid)
                # 检查是否是街镇节点
                level = record.get("n.level", 0)
                category = record.get("n.category", "")
                if level == 5 or category == "Town" or category == "街镇":
                    town_count += 1
                    print(f"找到街镇节点: {record['n.name']}, 层级: {level}, 类别: {category}")

                details = {
                    "code": safe_val(record.get("n.code", "-")),
                    "rank": safe_val(record.get("n.rank", 999)),
                    "total_siblings": cat_counts.get(record['n.parent'], 1) if record['n.level'] == 4 else 1,
                    "stars": safe_val(record.get("n.star_total", 0)),
                    "star_tech": safe_val(record.get("n.star_tech", 0)),
                    "star_str": safe_val(record.get("n.star_str", 0)),
                    "star_rel": safe_val(record.get("n.star_rel", 0)),
                    "intro": safe_val(record.get("n.intro", "暂无")),
                    "tech": safe_val(record.get("n.tech_text", "-")),
                    "scene": safe_val(record.get("n.scene_text", "-")),
                    "legal": safe_val(record.get("n.legal", "-")),
                    "capital": safe_val(record.get("n.capital", "-")),
                    "date": safe_val(record.get("n.date", "-")),
                    "insured": safe_val(record.get("n.insured", "-")),
                    "company_type": safe_val(record.get("n.company_type", "-")),
                    "industry_stream": safe_val(record.get("n.industry_stream", "-")),
                    "sub_category": safe_val(record.get("n.sub_category", "-")),
                    "confidence": safe_val(record.get("n.confidence", "-")),
                    "contact": safe_val(record.get("n.contact", "-")),
                    "address": safe_val(record.get("n.address", "-")),
                    "tags": safe_val(record.get("n.tags", "-")),
                    "product_services": safe_val(record.get("n.product_services", "-")),
                    "tech_text": safe_val(record.get("n.tech_text", "-")),
                    "scene_text": safe_val(record.get("n.scene_text", "-"))
                }

                nodes.append({
                    "id": str(nid),
                    "name": str(record["n.name"]),
                    "level": int(record["n.level"]),
                    "category": str(record["n.category"]),
                    "parent": str(record["n.parent"]) if record["n.parent"] else None,
                    "details": details
                })
            print(f"街镇节点总数: {town_count}")

            rel_result = session.run("MATCH (s)-[r]->(t) RETURN s.id, t.id")
            for record in rel_result:
                sid, tid = str(record["s.id"]), str(record["t.id"])
                if sid in seen_ids and tid in seen_ids:
                    links.append({"source": sid, "target": tid})

    except Exception as e:
        print(f"❌ Error: {e}")
        # 如果Neo4j连接失败，使用Excel数据作为备份
        print("⚠️  使用Excel数据作为备份")
        return get_data_from_excel()
    finally:
        driver.close()

    return {"nodes": nodes, "links": links}


@app.route('/')
def index(): return render_template('index.html')


@app.route('/api/data')
def get_data():
    try:
        # 尝试使用 Neo4j 获取数据
        data = get_data_from_neo4j()
        # 检查是否有街镇节点
        if 'nodes' in data:
            town_nodes = [n for n in data['nodes'] if n.get('level') == 5 or n.get('category') == 'Town']
            print(f"Neo4j 中的街镇节点数量: {len(town_nodes)}")
        return jsonify(data)
    except Exception as e:
        print(f"Neo4j 连接失败: {e}")
        # 返回模拟数据，包含街镇节点
        return jsonify({
            "nodes": [
                {"id": "ROOT", "name": "产业链图谱", "level": 1, "category": "Root", "parent": None, "details": {}},
                {"id": "stream_0", "name": "上游", "level": 2, "category": "Stream", "parent": "ROOT", "details": {}},
                {"id": "stream_1", "name": "中游", "level": 2, "category": "Stream", "parent": "ROOT", "details": {}},
                {"id": "stream_2", "name": "下游", "level": 2, "category": "Stream", "parent": "ROOT", "details": {}},
                {"id": "subcat_0", "name": "原材料", "level": 3, "category": "SubCategory", "parent": "stream_0",
                 "details": {}},
                {"id": "subcat_1", "name": "加工制造", "level": 3, "category": "SubCategory", "parent": "stream_1",
                 "details": {}},
                {"id": "subcat_2", "name": "销售服务", "level": 3, "category": "SubCategory", "parent": "stream_2",
                 "details": {}},
                {"id": "company_0", "name": "企业A", "level": 4, "category": "Company", "parent": "subcat_0",
                 "details": {}},
                {"id": "company_1", "name": "企业B", "level": 4, "category": "Company", "parent": "subcat_1",
                 "details": {}},
                {"id": "company_2", "name": "企业C", "level": 4, "category": "Company", "parent": "subcat_2",
                 "details": {}},
                {"id": "town_0", "name": "街镇1", "level": 5, "category": "Town", "parent": "ROOT", "details": {}},
                {"id": "town_1", "name": "街镇2", "level": 5, "category": "Town", "parent": "ROOT", "details": {}},
                {"id": "town_2", "name": "街镇3", "level": 5, "category": "Town", "parent": "ROOT", "details": {}}
            ],
            "links": [
                {"source": "stream_0", "target": "ROOT"},
                {"source": "stream_1", "target": "ROOT"},
                {"source": "stream_2", "target": "ROOT"},
                {"source": "subcat_0", "target": "stream_0"},
                {"source": "subcat_1", "target": "stream_1"},
                {"source": "subcat_2", "target": "stream_2"},
                {"source": "company_0", "target": "subcat_0"},
                {"source": "company_1", "target": "subcat_1"},
                {"source": "company_2", "target": "subcat_2"},
                {"source": "town_0", "target": "ROOT"},
                {"source": "town_1", "target": "ROOT"},
                {"source": "town_2", "target": "ROOT"},
                {"source": "company_0", "target": "town_0"},
                {"source": "company_1", "target": "town_1"},
                {"source": "company_2", "target": "town_2"}
            ]
        })


def get_relevant_info_from_neo4j(question):
    """从Neo4j获取与问题相关的信息"""
    driver = GraphDatabase.driver("neo4j://127.0.0.1:7687", auth=("neo4j", "zhihui123"))
    relevant_info = []

    try:
        with driver.session() as session:
            keywords = question.split()

            query1 = """
            MATCH (n) 
            WHERE ANY(keyword IN $keywords WHERE toLower(n.name) CONTAINS toLower(keyword))
            RETURN n.id, n.name, n.level, n.category, n.intro
            LIMIT 5
            """
            result1 = session.run(query1, keywords=keywords)

            for record in result1:
                node_info = {
                    "id": record["n.id"],
                    "name": record["n.name"],
                    "level": record["n.level"],
                    "category": record["n.category"],
                    "intro": record.get("n.intro", "")
                }
                relevant_info.append(node_info)

            query2 = """
            MATCH (s)-[r]->(t) 
            WHERE ANY(keyword IN $keywords WHERE toLower(s.name) CONTAINS toLower(keyword) OR toLower(t.name) CONTAINS toLower(keyword))
            RETURN s.name AS source, type(r) AS relationship, t.name AS target
            LIMIT 5
            """
            result2 = session.run(query2, keywords=keywords)

            for record in result2:
                rel_info = {
                    "source": record["source"],
                    "relationship": record["relationship"],
                    "target": record["target"]
                }
                relevant_info.append(rel_info)

    except Exception as e:
        print(f"Error querying Neo4j: {e}")
    finally:
        driver.close()

    return relevant_info


def get_enterprise_node_count():
    """从Neo4j获取企业节点数量"""
    driver = GraphDatabase.driver("neo4j://127.0.0.1:7687", auth=("neo4j", "zhihui123"))
    count = 0

    try:
        with driver.session() as session:
            query = """
            MATCH (n) 
            WHERE n.category = 'Company' OR n.level = 4
            RETURN count(n) AS count
            """
            result = session.run(query)

            for record in result:
                count = record["count"]

    except Exception as e:
        print(f"Error querying enterprise node count: {e}")
    finally:
        driver.close()

    return count


def get_enterprise_count_by_industry(industry):
    """从Neo4j获取特定产业环节的企业节点数量"""
    driver = GraphDatabase.driver("neo4j://127.0.0.1:7687", auth=("neo4j", "zhihui123"))
    count = 0

    try:
        with driver.session() as session:
            query = """
            MATCH (n) 
            WHERE (n.category = 'Company' OR n.level = 4) AND 
                  (n.industry_stream = $industry OR n.industry_stream CONTAINS $industry)
            RETURN count(n) AS count
            """
            result = session.run(query, industry=industry)

            for record in result:
                count = record["count"]

    except Exception as e:
        print(f"Error querying enterprise count by industry: {e}")
    finally:
        driver.close()

    return count


def get_enterprises_by_technology(technology):
    """从Neo4j获取特定技术领域的企业"""
    driver = GraphDatabase.driver("neo4j://127.0.0.1:7687", auth=("neo4j", "zhihui123"))
    enterprises = []

    try:
        with driver.session() as session:
            query = """
            MATCH (n) 
            WHERE (n.category = 'Company' OR n.level = 4) AND 
                  (toLower(n.name) CONTAINS toLower($technology) OR 
                   toLower(n.tech_text) CONTAINS toLower($technology) OR 
                   toLower(n.product_services) CONTAINS toLower($technology) OR 
                   toLower(n.intro) CONTAINS toLower($technology))
            RETURN n.id, n.name, n.level, n.category, n.intro
            LIMIT 10
            """
            result = session.run(query, technology=technology)

            for record in result:
                enterprise = {
                    "id": record["n.id"],
                    "name": record["n.name"],
                    "level": record["n.level"],
                    "category": record["n.category"],
                    "intro": record.get("n.intro", "")
                }
                enterprises.append(enterprise)

    except Exception as e:
        print(f"Error querying enterprises by technology: {e}")
    finally:
        driver.close()

    return enterprises


# 模拟生成回答
def generate_answer_with_dashscope(context, question):
    """模拟生成回答（实际应用中应使用真实的API）"""
    # 构建一个基于上下文和问题的模拟回答
    # 实际应用中，这里应该调用真实的LLM API

    # 简单的规则匹配，实际应用中应该使用更复杂的NLP处理
    # 先匹配更具体的规则
    if "上游有多少企业节点" in question or "上游有几个企业" in question or "上游企业数量" in question:
        # 从 Neo4j 查询上游企业节点数量
        count = get_enterprise_count_by_industry("上游")
        if count > 0:
            return f"您想了解的上游企业节点数量，根据海淀区产业图谱的数据，目前共有{count}个上游企业节点。这些企业主要分布在产业链的上游环节。"
        else:
            return "您想了解的上游企业节点数量，目前海淀区产业图谱中暂无上游企业节点数据。"
    elif "中游有多少企业节点" in question or "中游有几个企业" in question or "中游企业数量" in question:
        # 从 Neo4j 查询中游企业节点数量
        count = get_enterprise_count_by_industry("中游")
        if count > 0:
            return f"您想了解的中游企业节点数量，根据海淀区产业图谱的数据，目前共有{count}个中游企业节点。这些企业主要分布在产业链的中游环节。"
        else:
            return "您想了解的中游企业节点数量，目前海淀区产业图谱中暂无中游企业节点数据。"
    elif "下游有多少企业节点" in question or "下游有几个企业" in question or "下游企业数量" in question:
        # 从 Neo4j 查询下游企业节点数量
        count = get_enterprise_count_by_industry("下游")
        if count > 0:
            return f"您想了解的下游企业节点数量，根据海淀区产业图谱的数据，目前共有{count}个下游企业节点。这些企业主要分布在产业链的下游环节。"
        else:
            return "您想了解的下游企业节点数量，目前海淀区产业图谱中暂无下游企业节点数据。"
    elif "有几个企业节点" in question or "企业节点数量" in question or "多少企业" in question:
        # 从 Neo4j 查询真实的企业节点数量
        count = get_enterprise_node_count()
        if count > 0:
            return f"您想了解的企业节点数量，根据海淀区产业图谱的数据，目前共有{count}个企业节点。这些企业分布在不同的产业领域和街镇区域。"
        else:
            return "您想了解的企业节点数量，目前海淀区产业图谱中暂无企业节点数据。"

    elif "做" in question and "的企业" in question:
        # 提取技术领域
        import re
        match = re.search(r'做(.*?)的企业', question)
        if match:
            technology = match.group(1).strip()
            # 从 Neo4j 查询特定技术领域的企业
            enterprises = get_enterprises_by_technology(technology)
            if enterprises:
                enterprise_names = [e['name'] for e in enterprises]
                enterprise_list = '、'.join(enterprise_names)
                return f"根据海淀区产业图谱的数据，做{technology}的企业有：{enterprise_list}。您可以在图谱中查看这些企业的详细信息，包括基本信息、所属产业、技术特点等。"
            else:
                return f"目前海淀区产业图谱中暂未找到做{technology}的企业信息。"
        else:
            return "您想了解的企业信息可以在海淀区产业图谱中找到详细数据，包括企业基本信息、所属产业、技术特点等。"

    elif "企业" in question:
        return "您想了解的企业信息可以在海淀区产业图谱中找到详细数据，包括企业基本信息、所属产业、技术特点等。"
    elif "产业" in question:
        return "您关注的产业在海淀区发展良好，图谱中包含了该产业的上下游关系、重点企业分布等信息。"
    elif "街镇" in question:
        return "您提到的街镇在海淀区产业布局中具有重要地位，图谱中展示了该区域的企业分布和产业特色。"
    elif "关系" in question:
        return "您询问的关系信息在图谱中以连接的形式展示，反映了不同实体之间的关联。"
    else:
        return f"您的问题是关于'{question}'，海淀区产业图谱中包含了相关的详细信息。基于提供的上下文，您可以了解到更多相关内容。"


@app.route('/api/chat', methods=['POST'])
def chat():
    """处理智能体问答请求"""
    data = request.json
    question = data.get('question', '')

    if not question:
        return jsonify({"error": "请输入问题"}), 400

    try:
        # 从 Neo4j 获取相关信息
        relevant_info = get_relevant_info_from_neo4j(question)

        # 将相关信息转换为字符串作为上下文
        context = "\n".join([str(info) for info in relevant_info])

        # 如果没有找到相关信息，提供一个默认上下文
        if not context:
            context = "海淀区产业图谱包含了大量企业、产业和街镇等节点信息，以及它们之间的关系。"

        # 使用dashscope生成回答
        answer = generate_answer_with_dashscope(context, question)

        # 确保回答以"您"为主语
        if not answer.startswith('您'):
            # 简单处理，实际应用中可能需要更复杂的逻辑
            answer = "您" + answer

        return jsonify({"answer": answer})
    except Exception as e:
        print(f"Chat error: {e}")
        return jsonify({"error": "抱歉，处理您的问题时出错了"}), 500


if __name__ == '__main__':
    app.run(debug=True, port=5000)