import subprocess

# 執行 AWS CLI 指令並獲取結果
result = subprocess.run(["aws", "dynamodb", "list-tables"], capture_output=True, text=True)
print(result.stdout)
