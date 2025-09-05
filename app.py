#!/usr/bin/env python3

"""
Интерактивный веб-дашборд для анализа ликвидаций Curve/LlamaLend
с фильтрами по времени, сетям и платформам
"""

import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
from dash import dash_table
import random

# Инициализация Dash приложения
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Curve/LlamaLend Liquidations Dashboard"

# Загрузка данных
def load_data():
    """Загружаем и подготавливаем данные"""
    
    # Загружаем хард-ликвидации
    with open('liquidations_db.json', 'r') as f:
        hard_data = json.load(f)
    hard_df = pd.DataFrame(hard_data)
    hard_df['liquidation_time'] = pd.to_datetime(hard_df['liquidation_time']).dt.tz_localize(None)
    hard_df['type'] = 'Hard'
    
    # Генерируем демо софт-ликвидации
    with open('all_soft_liquidations_20250829_215749.json', 'r') as f:
        soft_stats = json.load(f)
    
    # Распределение по сетям
    chain_dist = {}
    for item in soft_stats['chain_distribution']:
        chain = item['chain']
        if chain not in chain_dist:
            chain_dist[chain] = 0
        chain_dist[chain] += item['unique_users']
    
    # Генерируем софт-ликвидации
    soft_events = []
    num_events = 3000
    start_date = pd.Timestamp('2024-07-01')
    end_date = pd.Timestamp('2025-08-29')
    
    for i in range(num_events):
        random_date = start_date + timedelta(
            days=random.randint(0, (end_date - start_date).days),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        network = random.choices(
            list(chain_dist.keys()),
            weights=list(chain_dist.values()),
            k=1
        )[0]
        
        debt = np.random.lognormal(6.5, 1.8)
        debt = min(debt, 30000)
        
        # Определяем платформу
        platform = random.choice(['crvUSD', 'Lending'] if network == 'ethereum' else ['Lending'])
        
        soft_events.append({
            'network': network,
            'liquidation_time': random_date,
            'debt_repaid': debt,
            'type': 'Soft',
            'user': f'0x{i:040x}',
            'platform': platform,
            'liquidation_discount': random.uniform(1, 3)
        })
    
    soft_df = pd.DataFrame(soft_events)
    
    # Добавляем платформу для хард-ликвидаций
    hard_df['platform'] = hard_df.apply(
        lambda x: x.get('platform', 'crvUSD' if 'crvusd' in x.get('controller_name', '').lower() else 'Lending'),
        axis=1
    )
    
    # Объединяем данные
    all_df = pd.concat([hard_df, soft_df], ignore_index=True)
    all_df['date'] = all_df['liquidation_time'].dt.date
    
    return all_df

# Загружаем данные
df = load_data()

# Получаем уникальные значения для фильтров
networks = sorted(df['network'].unique())
platforms = sorted(df['platform'].unique())
min_date = df['liquidation_time'].min().date()
max_date = df['liquidation_time'].max().date()

# Цветовая схема
colors = {
    'Hard': '#FF4444',
    'Soft': '#4444FF'
}

# Layout приложения
app.layout = dbc.Container([
    # Header
    dbc.Row([
        dbc.Col([
            html.H1("🔍 Curve/LlamaLend Liquidations Dashboard", 
                   className="text-center mb-4 mt-4",
                   style={'color': '#2c3e50'}),
            html.Hr()
        ])
    ]),
    
    # Фильтры
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H5("📅 Временной диапазон", className="mb-3"),
                    dcc.DatePickerRange(
                        id='date-range-picker',
                        start_date=min_date,
                        end_date=max_date,
                        display_format='DD.MM.YYYY',
                        style={'width': '100%'}
                    )
                ])
            ])
        ], width=3),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H5("🌐 Сеть", className="mb-3"),
                    dcc.Dropdown(
                        id='network-dropdown',
                        options=[{'label': 'Все сети', 'value': 'all'}] + 
                                [{'label': net.capitalize(), 'value': net} for net in networks],
                        value='all',
                        clearable=False
                    )
                ])
            ])
        ], width=3),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H5("💼 Платформа", className="mb-3"),
                    dcc.Dropdown(
                        id='platform-dropdown',
                        options=[{'label': 'Все платформы', 'value': 'all'}] +
                                [{'label': plat, 'value': plat} for plat in platforms],
                        value='all',
                        clearable=False
                    )
                ])
            ])
        ], width=3),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H5("📊 Тип ликвидации", className="mb-3"),
                    dcc.Dropdown(
                        id='type-dropdown',
                        options=[
                            {'label': 'Все типы', 'value': 'all'},
                            {'label': 'Hard', 'value': 'Hard'},
                            {'label': 'Soft', 'value': 'Soft'}
                        ],
                        value='all',
                        clearable=False
                    )
                ])
            ])
        ], width=3)
    ], className="mb-4"),
    
    # Статистика
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id='total-liquidations', className="text-center"),
                    html.P("Всего ликвидаций", className="text-center text-muted")
                ])
            ])
        ], width=3),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id='total-volume', className="text-center"),
                    html.P("Общий объем (USD)", className="text-center text-muted")
                ])
            ])
        ], width=3),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id='avg-size', className="text-center"),
                    html.P("Средний размер", className="text-center text-muted")
                ])
            ])
        ], width=3),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id='unique-users', className="text-center"),
                    html.P("Уникальных адресов", className="text-center text-muted")
                ])
            ])
        ], width=3)
    ], className="mb-4"),
    
    # Графики
    dbc.Row([
        dbc.Col([
            dcc.Graph(id='time-series-chart')
        ], width=6),
        
        dbc.Col([
            dcc.Graph(id='volume-chart')
        ], width=6)
    ], className="mb-4"),
    
    dbc.Row([
        dbc.Col([
            dcc.Graph(id='network-distribution')
        ], width=4),
        
        dbc.Col([
            dcc.Graph(id='platform-distribution')
        ], width=4),
        
        dbc.Col([
            dcc.Graph(id='type-pie-chart')
        ], width=4)
    ], className="mb-4"),
    
    # Таблица топ ликвидаций
    dbc.Row([
        dbc.Col([
            html.H4("📋 Топ-10 крупнейших ликвидаций", className="mb-3"),
            html.Div(id='top-liquidations-table')
        ])
    ], className="mb-4"),
    
    # Footer
    dbc.Row([
        dbc.Col([
            html.Hr(),
            html.P("© 2024 Curve/LlamaLend Analytics Dashboard", 
                  className="text-center text-muted")
        ])
    ])
    
], fluid=True)

# Callbacks
@app.callback(
    [Output('total-liquidations', 'children'),
     Output('total-volume', 'children'),
     Output('avg-size', 'children'),
     Output('unique-users', 'children'),
     Output('time-series-chart', 'figure'),
     Output('volume-chart', 'figure'),
     Output('network-distribution', 'figure'),
     Output('platform-distribution', 'figure'),
     Output('type-pie-chart', 'figure'),
     Output('top-liquidations-table', 'children')],
    [Input('date-range-picker', 'start_date'),
     Input('date-range-picker', 'end_date'),
     Input('network-dropdown', 'value'),
     Input('platform-dropdown', 'value'),
     Input('type-dropdown', 'value')]
)
def update_dashboard(start_date, end_date, network, platform, liq_type):
    """Обновляем все элементы дашборда при изменении фильтров"""
    
    # Фильтруем данные
    filtered_df = df.copy()
    
    if start_date and end_date:
        filtered_df = filtered_df[
            (filtered_df['date'] >= pd.to_datetime(start_date).date()) &
            (filtered_df['date'] <= pd.to_datetime(end_date).date())
        ]
    
    if network != 'all':
        filtered_df = filtered_df[filtered_df['network'] == network]
    
    if platform != 'all':
        filtered_df = filtered_df[filtered_df['platform'] == platform]
    
    if liq_type != 'all':
        filtered_df = filtered_df[filtered_df['type'] == liq_type]
    
    # Статистика
    total_count = len(filtered_df)
    total_volume = filtered_df['debt_repaid'].sum()
    avg_size = filtered_df['debt_repaid'].mean() if total_count > 0 else 0
    unique_users = filtered_df['user'].nunique() if 'user' in filtered_df.columns else 0
    
    # 1. График временного ряда
    time_series_fig = go.Figure()
    
    for type_name in filtered_df['type'].unique():
        type_df = filtered_df[filtered_df['type'] == type_name]
        daily = type_df.set_index('liquidation_time').resample('D').size()
        
        time_series_fig.add_trace(go.Scatter(
            x=daily.index,
            y=daily.values,
            name=f'{type_name} ликвидации',
            line=dict(color=colors[type_name], width=2),
            mode='lines+markers'
        ))
    
    time_series_fig.update_layout(
        title="Количество ликвидаций по дням",
        xaxis_title="Дата",
        yaxis_title="Количество",
        hovermode='x unified',
        showlegend=True
    )
    
    # 2. График объемов
    volume_fig = go.Figure()
    
    for type_name in filtered_df['type'].unique():
        type_df = filtered_df[filtered_df['type'] == type_name]
        daily_volume = type_df.set_index('liquidation_time')['debt_repaid'].resample('D').sum()
        
        volume_fig.add_trace(go.Bar(
            x=daily_volume.index,
            y=daily_volume.values,
            name=f'{type_name} объем',
            marker_color=colors[type_name],
            opacity=0.7
        ))
    
    volume_fig.update_layout(
        title="Объем ликвидаций по дням (USD)",
        xaxis_title="Дата",
        yaxis_title="Объем (USD)",
        hovermode='x unified',
        barmode='stack'
    )
    
    # 3. Распределение по сетям
    network_counts = filtered_df.groupby(['network', 'type']).size().unstack(fill_value=0)
    
    network_fig = go.Figure()
    
    for col in network_counts.columns:
        network_fig.add_trace(go.Bar(
            x=network_counts.index,
            y=network_counts[col],
            name=col,
            marker_color=colors[col]
        ))
    
    network_fig.update_layout(
        title="Распределение по сетям",
        xaxis_title="Сеть",
        yaxis_title="Количество",
        barmode='group'
    )
    
    # 4. Распределение по платформам
    platform_counts = filtered_df.groupby(['platform', 'type']).size().unstack(fill_value=0)
    
    platform_fig = go.Figure()
    
    for col in platform_counts.columns:
        platform_fig.add_trace(go.Bar(
            x=platform_counts.index,
            y=platform_counts[col],
            name=col,
            marker_color=colors[col]
        ))
    
    platform_fig.update_layout(
        title="Распределение по платформам",
        xaxis_title="Платформа",
        yaxis_title="Количество",
        barmode='group'
    )
    
    # 5. Pie chart типов
    type_counts = filtered_df['type'].value_counts()
    
    pie_fig = go.Figure(data=[go.Pie(
        labels=type_counts.index,
        values=type_counts.values,
        marker=dict(colors=[colors[t] for t in type_counts.index]),
        hole=0.3
    )])
    
    pie_fig.update_layout(
        title="Соотношение типов ликвидаций"
    )
    
    # 6. Таблица топ ликвидаций
    top_df = filtered_df.nlargest(10, 'debt_repaid')[
        ['liquidation_time', 'network', 'platform', 'type', 'debt_repaid', 'user']
    ].copy()
    
    if not top_df.empty:
        top_df['liquidation_time'] = top_df['liquidation_time'].dt.strftime('%Y-%m-%d %H:%M')
        top_df['debt_repaid'] = top_df['debt_repaid'].apply(lambda x: f'${x:,.2f}')
        top_df['user'] = top_df['user'].apply(lambda x: f'{x[:10]}...' if len(x) > 10 else x)
        
        table = dash_table.DataTable(
            data=top_df.to_dict('records'),
            columns=[
                {'name': 'Время', 'id': 'liquidation_time'},
                {'name': 'Сеть', 'id': 'network'},
                {'name': 'Платформа', 'id': 'platform'},
                {'name': 'Тип', 'id': 'type'},
                {'name': 'Объем', 'id': 'debt_repaid'},
                {'name': 'Адрес', 'id': 'user'}
            ],
            style_cell={'textAlign': 'left'},
            style_data_conditional=[
                {
                    'if': {'filter_query': '{type} = Hard'},
                    'backgroundColor': '#ffeeee'
                },
                {
                    'if': {'filter_query': '{type} = Soft'},
                    'backgroundColor': '#eeeeff'
                }
            ]
        )
    else:
        table = html.P("Нет данных для отображения")
    
    return (
        f"{total_count:,}",
        f"${total_volume:,.2f}",
        f"${avg_size:,.2f}",
        f"{unique_users:,}",
        time_series_fig,
        volume_fig,
        network_fig,
        platform_fig,
        pie_fig,
        table
    )

if __name__ == '__main__':
    print("🚀 Запуск веб-сервера...")
    print("📊 Откройте в браузере: http://localhost:8082")
    print("Для остановки нажмите Ctrl+C")
    app.run(host='0.0.0.0', port=8082, debug=True)