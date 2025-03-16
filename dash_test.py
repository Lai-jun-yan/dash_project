import dash
from dash import dcc, html, Input, Output, dash_table
import boto3
import pandas as pd

# 初始化 DynamoDB 客戶端
dynamodb = boto3.client("dynamodb")

# 建立 Dash 應用
app = dash.Dash(__name__)

# 取得所有表格
table_options = [{"label": table, "value": table} for table in dynamodb.list_tables()["TableNames"]]

app.layout = html.Div([
    html.H1("Amazon Web Services DynamoDB", style={"textAlign": "center", "color": "#333", "marginBottom": "20px"}),
    dcc.Dropdown(id="table-dropdown", options=table_options, placeholder="選擇 DynamoDB 表格", style={"width": "50%", "margin": "auto"}),
    html.Div(id="table-content", style={"marginTop": "20px", "textAlign": "center"})
], style={"fontFamily": "Arial, sans-serif", "width": "1000px", "margin": "auto", "padding": "20px", "boxShadow": "0px 4px 10px rgba(0, 0, 0, 0.1)"})

@app.callback(
    Output("table-content", "children"),
    Input("table-dropdown", "value")
)
def display_table(table_name):
    if not table_name:
        return "請選擇一個表格"
    
    # 獲取表格資料
    response = dynamodb.scan(TableName=table_name)
    items = response["Items"]

    # 轉換為 Pandas DataFrame（簡化顯示）
    df = pd.DataFrame([{k: list(v.values())[0] for k, v in item.items()} for item in items])
    
    return dash_table.DataTable(
        columns=[{"name": col, "id": col} for col in df.columns],
        data=df.to_dict("records"),
        page_size=10,  # 設定分頁，每頁顯示 10 筆資料
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": "#f4f4f4", "fontWeight": "bold"},
        style_cell={"border": "1px solid #ddd", "padding": "8px", "textAlign": "left"}
    )

if __name__ == "__main__":
    app.run_server(debug=True)
