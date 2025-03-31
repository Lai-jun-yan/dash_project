import dash
from dash import dcc, html, Input, Output, State, dash_table
import pandas as pd
import boto3
import io
import base64

# 初始化 Dash 應用
app = dash.Dash(__name__)

# 創建 AWS DynamoDB 資源
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

def create_table(dynamodb, table_name, column_names):
    Partition_Key = column_names[0]
    Sort_Key = column_names[1]
    other_columns = column_names[2:]
    
    key_schema = [
        {'AttributeName': Partition_Key, 'KeyType': 'HASH'},
        {'AttributeName': Sort_Key, 'KeyType': 'RANGE'}
    ]
    
    attribute_definitions = [
        {'AttributeName': Partition_Key, 'AttributeType': 'S'},
        {'AttributeName': Sort_Key, 'AttributeType': 'N'}
    ]
    
    for col in other_columns:
        attribute_definitions.append({'AttributeName': col, 'AttributeType': 'S'})
    
    global_secondary_indexes = []
    for col in other_columns:
        global_secondary_indexes.append({
            'IndexName': f"{col}_Index",
            'KeySchema': [
                {'AttributeName': col, 'KeyType': 'HASH'},
                {'AttributeName': Sort_Key, 'KeyType': 'RANGE'}
            ],
            'Projection': {'ProjectionType': 'ALL'},
            'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        })
    
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=key_schema,
        AttributeDefinitions=attribute_definitions,
        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5},
        GlobalSecondaryIndexes=global_secondary_indexes
    )
    
    return table

app.layout = html.Div([
    html.H1("AWS DynamoDB", style={'textAlign': 'center', 'marginBottom': '20px'}),
    
    dcc.Upload(
        id='upload-data',
        children=html.Button('選擇 CSV 檔案', style={'marginBottom': '10px'}),
        multiple=False
    ),
    
    html.Div(id='output-data-table', style={'marginBottom': '20px'}),
    
    html.Button('上傳至 DynamoDB', id='upload-button', style={'display': 'none'}),
    html.Div(id='upload-status', style={'marginTop': '20px'})
])

@app.callback(
    [Output('output-data-table', 'children'), Output('upload-button', 'style')],
    [Input('upload-data', 'contents')]
)
def show_dataframe(contents):
    if contents is None:
        return "", {'display': 'none'}
    
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
    
    table = dash_table.DataTable(
        columns=[{'name': col, 'id': col} for col in df.columns],
        data=df.to_dict('records'),
        page_size=10
    )
    
    return table, {'display': 'block'}

@app.callback(
    Output('upload-status', 'children'),
    Input('upload-button', 'n_clicks'),
    State('upload-data', 'contents'),
    State('upload-data', 'filename')
)
def upload_to_dynamodb(n_clicks, contents, filename):
    if n_clicks is None or contents is None:
        return ""
    
    try:
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
        column_names = df.columns.tolist()
        
        table_name = filename.split('.')[0]  # 取 CSV 檔案名稱作為表格名稱
        table = create_table(dynamodb, table_name, column_names)
        table.wait_until_exists()
        
        for i in range(len(df)):
            row = df.iloc[i]
            item = {}
            
            for col in column_names:
                if col == column_names[1]:
                    item[col] = int(row[col])
                else:
                    item[col] = str(row[col])
            table.put_item(Item=item)
        
        return f"資料已成功上傳到 DynamoDB (表格名稱: {table_name})！"
    except Exception as e:
        return f"上傳失敗: {str(e)}"

if __name__ == '__main__':
    app.run_server(debug=True)
