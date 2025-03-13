import dash
from dash import html, dcc, Input, Output, State, callback
import dash_bootstrap_components as dbc
import boto3
import json
from decimal import Decimal

# 初始化 Dash 應用程式
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
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

# 取得 DynamoDB 表格列表
try:
    table_options = [{"label": table, "value": table} for table in dynamodb_client.list_tables()['TableNames']]
except Exception:
    table_options = []

# 應用程式介面
app.layout = html.Div([
    dbc.Container([
        html.H1("DynamoDB 工具", className="my-4"),

        # 分頁
        dcc.Tabs(id="tabs", value="tab-query", children=[
            dcc.Tab(label="查詢表格", value="tab-query"),
            dcc.Tab(label="創建表格", value="tab-create"),
        ]),

        html.Div(id="tabs-content")
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


# **分頁內容回調**
@callback(
    Output("tabs-content", "children"),
    Input("tabs", "value")
)
def render_tab_content(tab):
    if tab == "tab-query":
        return html.Div([
            dbc.Card([
                dbc.CardBody([
                    html.H4("選擇表格", className="card-title"),
                    dbc.Select(id="table-select", options=table_options, placeholder="選擇表格"),
                    dbc.Button("查看表格內容", id="view-table-btn", color="success", className="mt-2"),
                    html.Div(id="table-content")
                ])
            ])
        ])

    elif tab == "tab-create":
        return html.Div([
            dbc.Card([
                dbc.CardBody([
                    html.H4("創建新表格", className="card-title"),

                    # 表格名稱輸入
                    dbc.Input(id="new-table-name", placeholder="輸入表格名稱", type="text", className="mb-2"),

                    # Attribute 名稱 & 類型
                    html.Div(id="attribute-container", children=[
                        dbc.Row([
                            dbc.Col(dbc.Input(placeholder="Attribute Name", type="text"), width=5),
                            dbc.Col(dbc.Select(
                                options=[
                                    {"label": "String", "value": "S"},
                                    {"label": "Number", "value": "N"},
                                    {"label": "Binary", "value": "B"}
                                ],
                                placeholder="選擇類型"
                            ), width=4),
                            dbc.Col(dbc.Button("❌", color="danger", className="remove-attr-btn"), width=1)
                        ], className="mb-2")
                    ]),

                    # 按鈕
                    dbc.Button("新增屬性", id="add-attribute-btn", color="secondary", className="mb-2"),
                    dbc.Button("創建表格", id="create-table-btn", color="primary", className="mb-2"),

                    # 創建結果
                    html.Div(id="create-table-status", className="mt-2")
                ])
            ])
        ])

# **允許新增多個 Attribute**
@callback(
    Output("attribute-container", "children"),
    Input("add-attribute-btn", "n_clicks"),
    State("attribute-container", "children"),
    prevent_initial_call=True
)
def add_attribute_field(n_clicks, existing_children):
    new_input = dbc.Row([
        dbc.Col(dbc.Input(placeholder="Attribute Name", type="text"), width=5),
        dbc.Col(dbc.Select(
            options=[
                {"label": "String", "value": "S"},
                {"label": "Number", "value": "N"},
                {"label": "Binary", "value": "B"}
            ],
            placeholder="選擇類型"
        ), width=4),
        dbc.Col(dbc.Button("❌", color="danger", className="remove-attr-btn"), width=1)
    ], className="mb-2")

    existing_children.append(new_input)
    return existing_children

# **創建表格回調**
@callback(
    Output("create-table-status", "children"),
    Input("create-table-btn", "n_clicks"),
    State("new-table-name", "value"),
    State("attribute-container", "children"),
    prevent_initial_call=True
)
def create_table(n_clicks, table_name, attributes):
    if not table_name:
        return "⚠️ 請輸入表格名稱！"

    if not attributes or len(attributes) == 0:
        return "⚠️ 至少需要一個 Attribute！"

    try:
        attribute_definitions = []
        key_schema = []
        
        for attr in attributes:
            attr_name = attr["props"]["children"][0]["props"]["children"]["props"]["value"]
            attr_type = attr["props"]["children"][1]["props"]["children"]["props"]["value"]

            if not attr_name or not attr_type:
                return "⚠️ 所有屬性名稱和類型都必須填寫！"

            attribute_definitions.append({"AttributeName": attr_name, "AttributeType": attr_type})

            # 第一個欄位為 Partition Key，第二個為 Sort Key（如果有的話）
            if len(key_schema) == 0:
                key_schema.append({"AttributeName": attr_name, "KeyType": "HASH"})
            elif len(key_schema) == 1:
                key_schema.append({"AttributeName": attr_name, "KeyType": "RANGE"})

        # 創建表格
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
        )
        return f"✅ 表格 '{table_name}' 創建成功！"

    except Exception as e:
        return f"❌ 創建表格失敗: {str(e)}"

# 啟動應用程式
if __name__ == '__main__':
    app.run_server(debug=True)
