import dash
from dash import html, dcc, Input, Output, State, callback, dash_table
import dash_bootstrap_components as dbc
import boto3
import pandas as pd
import json
from decimal import Decimal
import base64
import io

# 初始化 Dash 應用程式，加入 suppress_callback_exceptions=True
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
app.title = "DynamoDB 工具"

# AWS DynamoDB 客戶端
dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')

# 處理 Decimal 類型
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# **在應用啟動時載入表格列表**
try:
    table_options = [{"label": table, "value": table} for table in dynamodb_client.list_tables()['TableNames']]
except Exception as e:
    table_options = []

app.layout = html.Div([
    dbc.Container([  # 這部分就是所有內容區域
        html.H1("AWS DynamoDB跟中二小屁孩的照片", className="my-4"),

        # **分頁設置**
        dcc.Tabs(id="tabs", value="tab-query", children=[
            dcc.Tab(label="查詢表格", value="tab-query"),
            dcc.Tab(label="上傳表格", value="tab-upload")
        ]),

        html.Div(id="tabs-content")
    ], fluid=True),  # 這裡已經不需要再設定 className 了
], className="background-container")  # 保留 background-container 並設置背景圖片

# **分頁內容回調**
@callback(
    Output("tabs-content", "children"),
    Input("tabs", "value")
)
def render_tab_content(tab):
    if tab == "tab-query":
        return html.Div([
            dbc.Card([  # 查詢表格的內容
                dbc.CardBody([
                    html.H4("選擇表格", className="card-title"),
                    dbc.Row([
                        dbc.Col(
                            dbc.Select(
                                id="table-select",
                                options=table_options,  # **直接載入表格列表**
                                placeholder="選擇表格",
                                className="mb-2"
                            ),
                            width=12
                        ),
                    ]),
                    dbc.Button(
                        "查看表格內容", 
                        id="view-table-btn", 
                        color="success", 
                        className="mb-2"
                    ),
                ])
            ], className="mb-4"),

            dbc.Card([
                dbc.CardBody([
                    html.H4(id="table-header", children="表格內容"),
                    html.Div(id="table-data-container", children=[
                        dash_table.DataTable(
                            id="table-data",
                            columns=[],
                            data=[],
                            style_table={'overflowX': 'auto'},
                            style_cell={
                                'padding': '8px',
                                'textAlign': 'left'
                            },
                            style_header={
                                'backgroundColor': '#f8f9fa',
                                'fontWeight': 'bold'
                            },
                            page_size=10
                        )
                    ])
                ])
            ])
        ])
    elif tab == "tab-upload":
        return html.Div([  
            dbc.Card([  
                dbc.CardBody([  
                    html.H4("上傳 CSV 檔案", className="card-title"),
                    dcc.Upload(
                        id="upload-data",
                        children=html.Button('點擊此處選擇檔案'),
                        multiple=False
                    ),
                    html.Div(id="output-data-upload"),
                ])
            ], className="mb-4"),

            dbc.Card([  
                dbc.CardBody([  
                    html.H4(id="uploaded-table-header", children="上傳的表格內容"),
                    html.Div(id="uploaded-table-data-container", children=[
                        dash_table.DataTable(
                            id="uploaded-table-data",
                            columns=[],
                            data=[],
                            style_table={'overflowX': 'auto'},
                            style_cell={
                                'padding': '8px',
                                'textAlign': 'left'
                            },
                            style_header={
                                'backgroundColor': '#f8f9fa',
                                'fontWeight': 'bold'
                            },
                            page_size=10
                        ),
                        dbc.Button(
                            "新增此表到 AWS",
                            id="upload-to-dynamodb-btn",
                            color="primary",
                            className="mt-2",
                            style={'display': 'none'}  # 初始隱藏按鈕
                        )
                    ])
                ])
            ])
        ])

# **查詢表格內容回調**
@callback(
    Output("table-header", "children"),
    Output("table-data", "columns"),
    Output("table-data", "data"),
    Input("view-table-btn", "n_clicks"),
    State("table-select", "value"),
    prevent_initial_call=True
)
def view_table_content(n_clicks, table_name):
    if not table_name:
        return "請選擇表格", [], []
    
    try:
        table = dynamodb.Table(table_name)
        response = table.scan()
        items = response.get('Items', [])
        
        if not items:
            return f"表格 '{table_name}' 內容 (空表格)", [], []
        
        # 將數據轉換為DataFrame格式
        json_items = json.loads(json.dumps(items, cls=DecimalEncoder))
        df = pd.DataFrame(json_items)
        
        columns = [{"name": col, "id": col} for col in df.columns]
        data = df.to_dict('records')
        
        return f"表格 '{table_name}' 內容 ({len(data)} 筆資料)", columns, data
    
    except Exception as e:
        return f"查詢表格 '{table_name}' 失敗: {str(e)}", [], []

# **上傳 CSV 檔案回調**
@callback(
    Output("uploaded-table-header", "children"),
    Output("uploaded-table-data", "columns"),
    Output("uploaded-table-data", "data"),
    Output("upload-to-dynamodb-btn", "style"),  # 顯示/隱藏按鈕
    Input("upload-data", "contents"),
    State("upload-data", "filename"),
    prevent_initial_call=True
)
def upload_file(contents, filename):
    if contents is None:
        return "請上傳 CSV 檔案", [], [], {'display': 'none'}  # 隱藏按鈕

    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        # 嘗試讀取 CSV 檔案
        df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
        columns = [{"name": col, "id": col} for col in df.columns]
        data = df.to_dict('records')

        # 顯示「新增此表到 AWS」按鈕
        return f"上傳的表格: {filename} ({len(data)} 筆資料)", columns, data, {'display': 'inline-block'}

    except Exception as e:
        return f"讀取檔案失敗: {str(e)}", [], [], {'display': 'none'}


# 按鈕回調函數
@callback(
    Output("upload-to-dynamodb-btn", "children"),
    Input("upload-to-dynamodb-btn", "n_clicks"),
    State("uploaded-table-data", "data"),
    State("uploaded-table-data", "columns"),
    State("upload-data", "filename"),
    prevent_initial_call=True
)
def upload_to_dynamodb(n_clicks, data, columns, filename):
    if not data:
        return "沒有資料可以上傳"
    
    try:
        # 1. **表格名稱來自 CSV 檔案名稱**
        table_name = filename.split('.')[0]  # 去除副檔名，取得表格名稱
        
        # 2. **檢查表格是否已存在**
        existing_tables = dynamodb_client.list_tables()['TableNames']
        if table_name in existing_tables:
            return "表格已存在，請選擇其他名稱"
        
        # 3. **確保每筆資料都有唯一的 `ID` 作為 Partition Key**
        partition_key = "ID"
        for index, item in enumerate(data, start=1):  
            item[partition_key] = index  # 設置 ID 為唯一數字（從 1 開始遞增）

        # 4. **計算 ProvisionedThroughput（根據資料大小設置吞吐量）**
        read_capacity = len(data) // 100 + 1  # 每 100 筆資料設為 1 的讀取容量
        write_capacity = len(data) // 100 + 1  # 每 100 筆資料設為 1 的寫入容量

        # 5. **創建 DynamoDB 表格**
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': partition_key, 'KeyType': 'HASH'}  # Partition Key
            ],
            AttributeDefinitions=[
                {'AttributeName': partition_key, 'AttributeType': 'N'}  # `ID` 設為數字 (Number)
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': read_capacity,
                'WriteCapacityUnits': write_capacity
            }
        )

        # 6. **等待表格創建完成**
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

        # 7. **上傳資料到 DynamoDB**
        for item in data:
            table.put_item(Item={k: Decimal(v) if isinstance(v, (int, float)) else v for k, v in item.items()})

        return f"資料已成功上傳到 DynamoDB 表格 '{table_name}'！"
    
    except Exception as e:
        return f"上傳失敗: {str(e)}"
    
# **當切換到查詢表格時，更新可選的表格列表**
@callback(
    Output("table-select", "options"),
    Input("tabs", "value")
)
def update_table_options(tab):
    if tab == "tab-query":
        try:
            table_names = dynamodb_client.list_tables()['TableNames']
            return [{"label": table, "value": table} for table in table_names]
        except Exception as e:
            return []
    return dash.no_update  # 其他分頁時不更新


if __name__ == '__main__':
    app.run_server(debug=True)
