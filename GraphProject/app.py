import math
from flask import Flask, render_template, jsonify
from neo4j import GraphDatabase

app = Flask(__name__)
URI = "neo4j://127.0.0.1:7687"
AUTH = ("neo4j", "zhihui123")


def safe_val(val):
    if val is None: return ""
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)): return 0
    return val


def get_data_from_neo4j():
    driver = GraphDatabase.driver(URI, auth=AUTH)
    nodes = []
    links = []
    try:
        with driver.session() as session:
            # 1. 获取所有节点 (扩大 LIMIT 以容纳街镇)
            # 确保获取 Level 5 的街镇节点
            result = session.run("""
                MATCH (n) 
                RETURN n.id, n.name, n.level, n.category, n.parent, n.rank, n.code,
                       n.star_total, n.star_tech, n.star_str, n.star_rel,
                       n.intro, n.legal, n.capital, n.date, n.tech_text, n.scene_text,
                       n.confidence, n.insured, n.company_type, n.industry_stream, n.sub_category,
                       n.contact, n.address, n.tags, n.product_services
                LIMIT 10000
            """)

            # 预计算同类数量 (Top% 计算用)
            cat_counts = {}
            raw_records = list(result)
            for r in raw_records:
                if r['n.level'] == 4:
                    parent = r['n.parent']
                    cat_counts[parent] = cat_counts.get(parent, 0) + 1

            seen_ids = set()
            for record in raw_records:
                nid = record["n.id"]
                if nid in seen_ids: continue
                seen_ids.add(nid)

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

            # 2. 获取关系
            # 包含 (Company)-[:LOCATED_IN]->(Town)
            rel_result = session.run("MATCH (s)-[r]->(t) RETURN s.id, t.id")
            for record in rel_result:
                sid, tid = str(record["s.id"]), str(record["t.id"])
                if sid in seen_ids and tid in seen_ids:
                    links.append({"source": sid, "target": tid})

    except Exception as e:
        print(f"❌ Error: {e}")
        return {"error": str(e)}
    finally:
        driver.close()

    return {"nodes": nodes, "links": links}


@app.route('/')
def index(): return render_template('index.html')


@app.route('/api/data')
def get_data(): return jsonify(get_data_from_neo4j())


if __name__ == '__main__':
    app.run(debug=True, port=5000)