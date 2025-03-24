import dash
from dash import dcc, html, Input, Output
import pandas as pd
import boto3
import io
import base64

# 初始化 Dash 應用
app = dash.Dash(__name__)

# 創建 AWS DynamoDB 資源
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

# 創建表格的函數
def create_table(dynamodb, table_name, column_names):
    # 定義 Partition Key 和 Sort Key
    Partition_Key = column_names[0]
    Sort_Key = column_names[1]
    
    # 其他欄位的名稱
    other_columns = column_names[2:]
    
    # 定義 KeySchema 和 AttributeDefinitions
    key_schema = [
        {'AttributeName': Partition_Key, 'KeyType': 'HASH'},  # Partition Key
        {'AttributeName': Sort_Key, 'KeyType': 'RANGE'}  # Sort Key
    ]
    
    attribute_definitions = [
        {'AttributeName': Partition_Key, 'AttributeType': 'S'},
        {'AttributeName': Sort_Key, 'AttributeType': 'N'}
    ]
    
    # 加入其他欄位的 AttributeDefinitions
    for col in other_columns:
        attribute_definitions.append({'AttributeName': col, 'AttributeType': 'S'})
    
    # 設置二級索引
    global_secondary_indexes = []
    for col in other_columns:
        global_secondary_indexes.append({
            'IndexName': f"{col}_Index",  # 二級索引的名稱
            'KeySchema': [
                {'AttributeName': col, 'KeyType': 'HASH'},  # Partition Key for GSI
                {'AttributeName': Sort_Key, 'KeyType': 'RANGE'}  # Sort Key for GSI
            ],
            'Projection': {
                'ProjectionType': 'ALL'  # 返回所有欄位
            },
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        })
    
    # 創建表格
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=key_schema,
        AttributeDefinitions=attribute_definitions,
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        },
        GlobalSecondaryIndexes=global_secondary_indexes
    )
    
    return table

# 設計應用介面
app.layout = html.Div([
    # 上傳按鈕
    dcc.Upload(
        id='upload-data',
        children=html.Button('選擇 CSV 檔案'),
        multiple=False
    ),
    html.Div(id='upload-status', style={'margin-top': '20px'})
])

# 上傳資料到 DynamoDB 的回調
@app.callback(
    Output('upload-status', 'children'),
    Input('upload-data', 'contents')
)
def upload_to_dynamodb(contents):
    if contents is None:
        return ""

    try:
        # 解碼 CSV 檔案內容
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
        
        column_names = df.columns.tolist()

        # 呼叫 create_table 函數來創建表格
        table = create_table(dynamodb, 'updated_rice_growth', column_names)

        # 等待表格創建完成
        table.wait_until_exists()

        # 插入資料到表格
        for i in range(len(df)):
            row = df.iloc[i]
            table.put_item(
                Item={
                    column_names[0]: str(row[column_names[0]]),
                    column_names[1]: int(row[column_names[1]]),
                    column_names[2]: str(row[column_names[2]]),
                    column_names[3]: str(row[column_names[3]]),
                    column_names[4]: str(row[column_names[4]])
                }
            )

        return "資料已成功上傳到 DynamoDB！"

    except Exception as e:
        return f"上傳失敗: {str(e)}"


if __name__ == '__main__':
    app.run_server(debug=True)
