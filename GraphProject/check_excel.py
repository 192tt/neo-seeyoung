import pandas as pd

# 读取Excel文件
file_path = "全部信息_全(1).xlsx"
df = pd.read_excel(file_path)

# 打印表头
print("Excel表头:")
print(df.columns.tolist())
print()

# 检查下游的数据
print("下游的数据:")
downstream_df = df[df['上中下游'] == '下游']
print(f"下游数据条数: {len(downstream_df)}")
print()

# 检查细分小类字段
if '细分小类' in df.columns:
    print("细分小类字段数据示例:")
    # 打印前10行细分小类数据
    for i, val in enumerate(df['细分小类'].head(10)):
        print(f"{i+1}: {val}")
    print()
    
    # 检查下游的细分小类数据
    print("下游的细分小类数据示例:")
    if len(downstream_df) > 0:
        for i, val in enumerate(downstream_df['细分小类'].head(10)):
            print(f"{i+1}: {val}")
    else:
        print("没有下游数据")
else:
    print("细分小类字段不存在")

# 检查是否包含关键字段
key_fields = ["核心技术/产品/服务", "主要应用场景", "公司简介", "经营范围"]
print("\n关键字段存在情况:")
for field in key_fields:
    exists = field in df.columns
    print(f"{field}: {'存在' if exists else '不存在'}")
