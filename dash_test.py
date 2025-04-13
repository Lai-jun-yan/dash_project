import dash
from dash import dcc, html, Input, Output, State, dash_table, ctx
import pandas as pd
import boto3
import io
import base64


# 初始化 Dash 應用
app = dash.Dash(__name__, suppress_callback_exceptions=True)

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

#介面顯示
app.layout = html.Div([
    html.H1("AWS DynamoDB", style={'textAlign': 'center', 'marginBottom': '20px'}),
    
    dcc.Tabs(id='tabs', value='tab-1', children=[
        dcc.Tab(label='歡迎頁', value='tab-1'),
        dcc.Tab(label='CSV 上傳', value='tab-2'),
    ]),

    html.Div(id='tabs-content')
], style={
    'fontFamily': 'Arial, sans-serif',
    'backgroundColor': '#f4f6f8',
    'minHeight': '100vh',
    'padding': '30px'
})

#選擇分頁函式
@app.callback(
    Output('tabs-content', 'children'),
    Input('tabs', 'value')
)
def render_tab_content(tab):
    if tab == 'tab-1':
        table_names = list(dynamodb.tables.all())
        options = [{'label': t.name, 'value': t.name} for t in table_names]

        return html.Div([
            html.Div([
                html.H2("🎉 歡迎使用本網站！", style={'textAlign': 'center'}),
                html.P("請從下方選擇 DynamoDB 表格以查看內容：", style={'textAlign': 'center'}),
                dcc.Dropdown(
                    id='table-dropdown',
                    options=options,
                    placeholder='選擇一個 DynamoDB 表格',
                    style={'width': '60%', 'margin': '20px auto'}
                ),
                html.Div(id='table-info')
            ], style={
                'backgroundColor': 'white',
                'borderRadius': '15px',
                'boxShadow': '0 4px 8px rgba(0, 0, 0, 0.1)',
                'padding': '30px',
                'maxWidth': '1000px',
                'margin': '0 auto'
            })
        ])

    
    elif tab == 'tab-2':
        return html.Div([
            html.Div([
                html.H3("📤 上傳 CSV 檔案到 DynamoDB", style={'textAlign': 'center'}),
                dcc.Upload(
                    id='upload-data',
                    children=html.Div([
                        html.Button('選擇 CSV 檔案', style={
                            'backgroundColor': '#28a745',
                            'color': 'white',
                            'padding': '10px 20px',
                            'border': 'none',
                            'borderRadius': '8px',
                            'cursor': 'pointer'
                        })
                    ]),
                    multiple=False,
                    style={'textAlign': 'center', 'marginBottom': '20px'}
                ),
                html.Div(id='output-data-table'),
                html.Div([
                    html.Button('📤 上傳至 DynamoDB', id='upload-button',
                                style={'display': 'none', 'backgroundColor': '#007bff', 'color': 'white',
                                    'padding': '10px 20px', 'border': 'none',
                                    'borderRadius': '8px', 'cursor': 'pointer'})
                ], style={'textAlign': 'center'}),
                html.Div(id='upload-status', style={'marginTop': '20px', 'textAlign': 'center'})
            ], style={
                'backgroundColor': 'white',
                'borderRadius': '15px',
                'boxShadow': '0 4px 8px rgba(0, 0, 0, 0.1)',
                'padding': '30px',
                'maxWidth': '1000px',
                'margin': '0 auto'
            })
        ])


#查看已經存在的TABLE資訊
@app.callback(
    Output('table-info', 'children'),
    Input('table-dropdown', 'value')
)
def show_table_content(table_name):
    if table_name is None:
        return ""

    try:
        table = dynamodb.Table(table_name)
        table.load()  # 讀取最新的 table metadata

        # 取得基本結構
        key_schema = table.key_schema
        attr_defs = table.attribute_definitions
        gsi = table.global_secondary_indexes or []

        # 取得資料內容
        response = table.scan()
        items = response.get('Items', [])

        # 建立欄位資訊文字
        def get_key_type(attr_name):
            for key in key_schema:
                if key['AttributeName'] == attr_name:
                    return key['KeyType']
            return ''

        attr_table = dash_table.DataTable(
            columns=[
                {'name': 'Attribute Name', 'id': 'AttributeName'},
                {'name': 'Type', 'id': 'AttributeType'},
                {'name': 'Key Type', 'id': 'KeyType'}
            ],
            data=[
                {
                    'AttributeName': attr['AttributeName'],
                    'AttributeType': attr['AttributeType'],
                    'KeyType': get_key_type(attr['AttributeName'])
                } for attr in attr_defs
            ],
            style_table={'marginBottom': '20px'}
        )

        # 資料內容表格
        item_table = None
        if items:
            df = pd.DataFrame(items)
            item_table = dash_table.DataTable(
                columns=[{'name': col, 'id': col} for col in df.columns],
                data=df.to_dict('records'),
                page_size=10,
                style_table={'overflowX': 'auto'}
            )
        else:
            item_table = html.P("這個表格目前沒有資料。")

        return html.Div([
            html.Div([
                html.H4(f"📄 表格名稱：{table_name}", style={'color': '#333'}),
                html.H5("🔑 表格結構", style={'marginTop': '20px'}),
                attr_table,
                html.H5("📦 表格內容", style={'marginTop': '30px'}),
                item_table,
                html.Br(),
                dcc.Download(id="download-csv"),
                html.Button("📥 下載表格 CSV", id="download-button", n_clicks=0,
                            style={'marginTop': '20px', 'backgroundColor': '#007bff', 'color': 'white', 'border': 'none',
                                'padding': '10px 20px', 'borderRadius': '8px', 'cursor': 'pointer'}) if items else None
            ], style={
                'backgroundColor': '#fff',
                'padding': '30px',
                'borderRadius': '15px',
                'boxShadow': '0 2px 6px rgba(0, 0, 0, 0.1)'
            })
        ])

    except Exception as e:
        return html.P(f"讀取表格時發生錯誤：{str(e)}")


#上傳TABLE
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

@app.callback(
    Output("download-csv", "data"),
    Input("download-button", "n_clicks"),
    State("table-dropdown", "value"),
    prevent_initial_call=True
)
def download_table_as_csv(n_clicks, table_name):
    if not table_name:
        return dash.no_update

    table = dynamodb.Table(table_name)
    response = table.scan()
    items = response.get('Items', [])
    
    if not items:
        return dash.no_update

    df = pd.DataFrame(items)
    return dcc.send_data_frame(df.to_csv, filename=f"{table_name}.csv", index=False)

if __name__ == '__main__':
    app.run_server(debug=True)
