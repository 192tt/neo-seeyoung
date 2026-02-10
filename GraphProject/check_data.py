from neo4j import GraphDatabase

# 连接到Neo4j数据库
uri = "neo4j://127.0.0.1:7687"
auth = ("neo4j", "zhihui123")
driver = GraphDatabase.driver(uri, auth=auth)

with driver.session() as session:
    # 检查所有二级节点
    print("所有二级节点:")
    result = session.run('MATCH (n:Node {level: 2}) RETURN n.id, n.name')
    for record in result:
        print(f"ID: {record[0]}, Name: {record[1]}")
    
    # 检查所有三级节点
    print("\n所有三级节点:")
    result = session.run('MATCH (n:Node {level: 3}) RETURN n.id, n.name, n.parent LIMIT 20')
    for record in result:
        print(f"ID: {record[0]}, Name: {record[1]}, Parent: {record[2]}")
    
    # 检查所有四级节点（企业）
    print("\n所有四级节点（企业）:")
    result = session.run('MATCH (n:Node {level: 4}) RETURN n.id, n.name, n.parent LIMIT 10')
    for record in result:
        print(f"ID: {record[0]}, Name: {record[1]}, Parent: {record[2]}")

# 关闭连接
driver.close()
