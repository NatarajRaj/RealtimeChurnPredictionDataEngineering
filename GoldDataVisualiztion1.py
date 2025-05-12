import matplotlib.pyplot as plt
import seaborn as sns
import dash
from dash import dcc, html
import plotly.express as px
from dash.dependencies import Input, Output
from Gold1 import process_data

df = process_data()
app_features_pd = df["app_features_pd"]
payment_declines_pd = df["payment_declines_pd"]
arpu_df_pd = df["arpu_df_pd"]
support_features_pd = df["support_features_pd"]
enriched_df_pd = df["enriched_df_pd"]

# Visualize App Features: 'login_gap_days' vs 'total_sessions_last_30d'
plt.figure(figsize=(10, 6))
sns.scatterplot(data=app_features_pd, x="login_gap_days", y="total_sessions_last_30d", hue="customer_id")
plt.title("App Features: Login Gap Days vs Total Sessions Last 30 Days")
plt.xlabel("Login Gap Days")
plt.ylabel("Total Sessions Last 30 Days")
plt.show()

# Visualize Payment Declines: 'total_declined_payments' vs 'avg_payment_value'
plt.figure(figsize=(10, 6))
sns.scatterplot(data=payment_declines_pd, x="total_declined_payments", y="avg_payment_value", hue="customer_id")
plt.title("Payment Declines: Total Declined Payments vs Average Payment Value")
plt.xlabel("Total Declined Payments")
plt.ylabel("Average Payment Value")
plt.show()

# Visualize ARPU: 'total_revenue' vs 'total_sessions_last_30d'
plt.figure(figsize=(10, 6))
sns.scatterplot(data=arpu_df_pd, x="total_revenue", y="total_sessions_last_30d", hue="customer_id")
plt.title("ARPU: Total Revenue vs Total Sessions Last 30 Days")
plt.xlabel("Total Revenue")
plt.ylabel("Total Sessions Last 30 Days")
plt.show()

# Visualize Support Features: 'avg_resolution_time' vs 'avg_ticket_sentiment'
plt.figure(figsize=(10, 6))
sns.scatterplot(data=support_features_pd, x="avg_resolution_time", y="avg_ticket_sentiment", hue="customer_id")
plt.title("Support Features: Average Resolution Time vs Average Ticket Sentiment")
plt.xlabel("Average Resolution Time")
plt.ylabel("Average Ticket Sentiment")
plt.show()

# Visualize Enriched Data: 'signup_days_ago' vs 'login_gap_days'
plt.figure(figsize=(10, 6))
sns.scatterplot(data=enriched_df_pd, x="signup_days_ago", y="login_gap_days", hue="customer_id")
plt.title("Enriched Data: Signup Days Ago vs Login Gap Days")
plt.xlabel("Signup Days Ago")
plt.ylabel("Login Gap Days")
plt.show()

#  more enhanced on data visualization
# Initialize Dash app
app = dash.Dash(__name__)

# Define the app layout
app.layout = html.Div([
    html.H1("Customer Behavior Dashboard", style={'text-align': 'center'}),

    dcc.Dropdown(
        id='chart-dropdown',
        options=[
            {'label': 'Signup Days vs Login Gap', 'value': 'signup_login'},
            {'label': 'Sessions vs Declined Payments', 'value': 'sessions_declines'},
            {'label': 'Revenue vs Sessions', 'value': 'revenue_sessions'}
        ],
        value='signup_login',
        style={'width': '50%', 'margin': '20px auto'}
    ),

    dcc.Graph(id='chart-output')
])

# Callback to update graph
@app.callback(
    Output('chart-output', 'figure'),
    Input('chart-dropdown', 'value')
)
def update_graph(selected_chart):
    if selected_chart == 'signup_login':
        fig = px.scatter(enriched_df_pd, x='signup_days_ago', y='login_gap_days',
                         color='customer_id', title="Signup Days vs Login Gap Days",
                         labels={"signup_days_ago": "Signup Days Ago", "login_gap_days": "Login Gap Days"})
    elif selected_chart == 'sessions_declines':
        fig = px.scatter(enriched_df_pd, x='total_sessions_last_30d', y='total_declined_payments',
                         color='customer_id', title="Sessions vs Declined Payments",
                         labels={"total_sessions_last_30d": "Sessions (30d)", "total_declined_payments": "Declined Payments"})
    elif selected_chart == 'revenue_sessions':
        fig = px.scatter(enriched_df_pd, x='total_sessions_last_30d', y='avg_payment_value',
                         color='customer_id', title="Revenue vs Sessions",
                         labels={"total_sessions_last_30d": "Total Sessions", "avg_payment_value": "Avg Payment Value"})
    else:
        fig = px.scatter(title="No Data Available")

    return fig
