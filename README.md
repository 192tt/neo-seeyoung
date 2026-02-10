# neo-seeyoung
# ================= 配置 =================
# 阿里云 DashScope 配置
dashscope.api_key = 
MODEL_NAME = "qwen-plus"  # 使用能力更强的 Plus 模型

URI = "neo4j://127.0.0.1:7687"
AUTH = ("neo4j", "zhihui123")
FILE_NAME = "全部信息_全(1).xlsx"
配置如上（7，8行是数据库配置）
先运行import文件录入数据库，再运行app.py文件启动后端
