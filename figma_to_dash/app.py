import dash
from dash import html, dcc, Input, Output, State, callback, dash_table
import dash_bootstrap_components as dbc
import boto3
import pandas as pd
import json
from decimal import Decimal

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

if __name__ == '__main__':
    app.run_server(debug=True)
