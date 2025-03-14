import dash
from dash import html, dcc, Input, Output, State, callback, dash_table
import dash_bootstrap_components as dbc
import boto3
import pandas as pd
import json
from decimal import Decimal
import subprocess

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

# 應用程式介面
app.layout = html.Div([
    dbc.Container([
        html.H1("DynamoDB 工具", className="my-4"),

        # **分頁設置**
        dcc.Tabs(id="tabs", value="tab-query", children=[
            dcc.Tab(label="查詢表格", value="tab-query"),
            dcc.Tab(label="修改表格", value="tab-modify")  # 新增的分頁
        ]),

        html.Div(id="tabs-content")
    ])
])


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
    
    elif tab == "tab-modify":  # 新增修改表格的內容
        return html.Div([
            dbc.Card([
                dbc.CardBody([
                    html.H4("選擇表格", className="card-title"),
                    dbc.Row([
                        dbc.Col(
                            dbc.Select(
                                id="modify-table-select",
                                options=table_options,  # 直接載入表格列表
                                placeholder="選擇表格",
                                className="mb-2"
                            ),
                            width=12
                        ),
                    ]),
                    html.Br(),

                    # 表單來輸入物件的屬性
                    html.H4("新增物件", className="card-title"),
                    dbc.Row([
                        dbc.Col(dbc.Input(id="student-id", placeholder="Student ID", type="text"), width=6),
                        dbc.Col(dbc.Input(id="birthday", placeholder="Birthday (yyyy-mm-dd)", type="text"), width=6),
                        dbc.Col(dbc.Input(id="height", placeholder="Height (cm)", type="text"), width=6),
                    ], className="mb-2"),

                    dbc.Button("新增物件", id="add-item-btn", color="primary", className="mb-2"),
                    html.Div(id="item-status")
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


# **新增物件回調**
@callback(
    Output("item-status", "children"),
    Input("add-item-btn", "n_clicks"),
    State("modify-table-select", "value"),
    State("student-id", "value"),
    State("birthday", "value"),
    State("height", "value"),
    prevent_initial_call=True
)
def add_item_to_table(n_clicks, table_name, student_id, birthday, height):
    if not table_name or not student_id or not birthday or not height:
        return "請填寫所有欄位！"
    
    try:
        # 確保資料是字串格式
        student_id = str(student_id).strip()
        birthday = str(birthday).strip()
        height = str(height).strip()
        
        # 檢查資料是否符合預期格式
        if not student_id or not birthday or not height:
            return "請確保每個欄位都有填寫且格式正確！"
        
        # 生成 DynamoDB put-item JSON 結構
        item = {
            "StudentID": {"S": student_id},
            "Birthday": {"S": birthday},
            "Height": {"S": height}
        }
        
        # 轉換為 JSON 字符串
        item_str = json.dumps(item, separators=(',', ':'))
        
        # 把 JSON 字串中的雙引號替換為兩個雙引號（轉義）
        item_str_escaped = item_str.replace('"', '""')
        
        # 生成 AWS CLI 指令
        aws_command = f"aws dynamodb put-item --table-name {table_name} --item \"{item_str_escaped}\""
        
        # 使用 subprocess 執行指令
        result = subprocess.run(aws_command, shell=True, capture_output=True, text=True)
        
        # 檢查命令執行結果
        if result.returncode == 0:
            return f"成功新增物件：{student_id} 到表格 '{table_name}'"
        else:
            return f"新增物件失敗: {result.stderr}"
    
    except Exception as e:
        return f"新增物件失敗: {str(e)}"





if __name__ == '__main__':
    app.run_server(debug=True)