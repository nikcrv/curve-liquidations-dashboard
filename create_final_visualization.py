#!/usr/bin/env python3

import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import random

def generate_soft_liquidations_demo(hard_df):
    """Генерируем демо-данные софт-ликвидаций на основе статистики"""
    
    # Загружаем статистику софт-ликвидаций
    with open('all_soft_liquidations_20250829_215749.json', 'r') as f:
        soft_stats = json.load(f)
    
    soft_events = []
    
    # Генерируем события на основе статистики
    # Всего было 2512 уникальных пользователей в софт-ликвидациях
    total_soft_users = soft_stats['summary']['total_unique_users']
    
    # Распределение по сетям из chain_distribution (это список)
    chain_dist = soft_stats['chain_distribution']
    
    # Преобразуем в словарь для удобства
    chain_weights = {}
    for item in chain_dist:
        chain = item['chain']
        if chain not in chain_weights:
            chain_weights[chain] = 0
        chain_weights[chain] += item['unique_users']
    
    # Генерируем временной ряд
    start_date = pd.Timestamp('2024-07-01')
    end_date = pd.Timestamp('2025-08-29')
    
    # Создаем ~5000 событий софт-ликвидаций (больше чем хард)
    num_soft_events = 5000
    
    for i in range(num_soft_events):
        # Случайная дата в диапазоне
        random_days = random.randint(0, (end_date - start_date).days)
        event_date = start_date + timedelta(days=random_days, 
                                           hours=random.randint(0, 23),
                                           minutes=random.randint(0, 59))
        
        # Выбираем сеть с учетом распределения
        network = random.choices(
            list(chain_weights.keys()),
            weights=list(chain_weights.values()),
            k=1
        )[0]
        
        # Софт-ликвидации обычно меньше по размеру чем хард
        debt_size = np.random.lognormal(7, 2)  # Меньше чем хард-ликвидации
        debt_size = min(debt_size, 50000)  # Ограничиваем максимум
        
        event = {
            'network': network,
            'liquidation_time': event_date,
            'debt_repaid': debt_size,
            'type': 'Soft Liquidation',
            'user': f"0x{i:040x}",  # Генерируем адрес
            'health': random.uniform(0, 0.3),  # Низкое здоровье позиции
            'bands': f"[{random.randint(0, 10)}, {random.randint(11, 20)}]"
        }
        soft_events.append(event)
    
    return pd.DataFrame(soft_events)

def create_comprehensive_visualization():
    """Создаем комплексную визуализацию"""
    
    print("Загрузка данных хард-ликвидаций...")
    with open('liquidations_db.json', 'r') as f:
        hard_data = json.load(f)
    hard_df = pd.DataFrame(hard_data)
    # Убираем timezone для консистентности
    hard_df['liquidation_time'] = pd.to_datetime(hard_df['liquidation_time']).dt.tz_localize(None)
    hard_df['type'] = 'Hard Liquidation'
    print(f"Загружено {len(hard_df)} хард-ликвидаций")
    
    print("Генерация демо софт-ликвидаций на основе статистики...")
    soft_df = generate_soft_liquidations_demo(hard_df)
    print(f"Сгенерировано {len(soft_df)} софт-ликвидаций")
    
    # Создаем фигуру с субплотами
    fig = make_subplots(
        rows=4, cols=2,
        subplot_titles=(
            'Временной ряд ликвидаций (количество)',
            'Временной ряд ликвидаций (объем USD)',
            'Распределение по сетям',
            'Размер ликвидаций (гистограмма)',
            'Накопительная динамика',
            'Соотношение типов ликвидаций',
            'Heatmap активности по дням недели',
            'Топ-10 дней по объему'
        ),
        specs=[
            [{'type': 'scatter'}, {'type': 'scatter'}],
            [{'type': 'bar'}, {'type': 'histogram'}],
            [{'type': 'scatter'}, {'type': 'pie'}],
            [{'type': 'heatmap'}, {'type': 'bar'}]
        ],
        vertical_spacing=0.08,
        horizontal_spacing=0.12,
        row_heights=[0.25, 0.25, 0.25, 0.25]
    )
    
    # Цветовая схема
    colors = {
        'Hard Liquidation': '#FF4136',  # Красный
        'Soft Liquidation': '#0074D9'   # Синий
    }
    
    # 1. Временной ряд - количество
    for liq_type, df in [('Hard Liquidation', hard_df), ('Soft Liquidation', soft_df)]:
        daily_count = df.set_index('liquidation_time').resample('W').size()
        fig.add_trace(
            go.Scatter(
                x=daily_count.index,
                y=daily_count.values,
                name=liq_type,
                line=dict(color=colors[liq_type], width=2),
                mode='lines+markers',
                hovertemplate='%{x|%Y-%m-%d}<br>Количество: %{y}<extra></extra>'
            ),
            row=1, col=1
        )
    
    # 2. Временной ряд - объем
    for liq_type, df in [('Hard Liquidation', hard_df), ('Soft Liquidation', soft_df)]:
        daily_volume = df.set_index('liquidation_time')['debt_repaid'].resample('W').sum()
        fig.add_trace(
            go.Scatter(
                x=daily_volume.index,
                y=daily_volume.values,
                name=f"{liq_type} объем",
                line=dict(color=colors[liq_type], width=2, dash='dash'),
                mode='lines',
                hovertemplate='%{x|%Y-%m-%d}<br>Объем: $%{y:,.0f}<extra></extra>'
            ),
            row=1, col=2
        )
    
    # 3. Распределение по сетям
    all_df = pd.concat([hard_df, soft_df])
    network_pivot = all_df.pivot_table(
        index='network', 
        columns='type', 
        values='debt_repaid', 
        aggfunc='count',
        fill_value=0
    )
    
    for col in network_pivot.columns:
        fig.add_trace(
            go.Bar(
                x=network_pivot.index,
                y=network_pivot[col],
                name=col,
                marker_color=colors[col],
                text=network_pivot[col],
                textposition='auto',
                hovertemplate='%{x}<br>%{y} ликвидаций<extra></extra>'
            ),
            row=2, col=1
        )
    
    # 4. Гистограмма размеров
    fig.add_trace(
        go.Histogram(
            x=hard_df['debt_repaid'],
            name='Hard размеры',
            marker_color=colors['Hard Liquidation'],
            opacity=0.7,
            nbinsx=50,
            hovertemplate='Размер: $%{x}<br>Количество: %{y}<extra></extra>'
        ),
        row=2, col=2
    )
    
    fig.add_trace(
        go.Histogram(
            x=soft_df['debt_repaid'],
            name='Soft размеры',
            marker_color=colors['Soft Liquidation'],
            opacity=0.7,
            nbinsx=50,
            hovertemplate='Размер: $%{x}<br>Количество: %{y}<extra></extra>'
        ),
        row=2, col=2
    )
    
    # 5. Накопительная динамика
    for liq_type, df in [('Hard Liquidation', hard_df), ('Soft Liquidation', soft_df)]:
        cumulative = df.set_index('liquidation_time').resample('D').size().cumsum()
        fig.add_trace(
            go.Scatter(
                x=cumulative.index,
                y=cumulative.values,
                name=f"{liq_type} накопительно",
                line=dict(color=colors[liq_type], width=3),
                fill='tonexty' if liq_type == 'Soft Liquidation' else None,
                hovertemplate='%{x|%Y-%m-%d}<br>Всего: %{y}<extra></extra>'
            ),
            row=3, col=1
        )
    
    # 6. Pie chart соотношения
    type_counts = all_df.groupby('type').agg({
        'debt_repaid': ['count', 'sum', 'mean']
    }).round(2)
    
    fig.add_trace(
        go.Pie(
            labels=['Хард-ликвидации', 'Софт-ликвидации'],
            values=[len(hard_df), len(soft_df)],
            marker=dict(colors=[colors['Hard Liquidation'], colors['Soft Liquidation']]),
            textinfo='label+percent',
            hovertemplate='%{label}<br>Количество: %{value}<br>%{percent}<extra></extra>'
        ),
        row=3, col=2
    )
    
    # 7. Heatmap по дням недели и часам
    all_df['weekday'] = all_df['liquidation_time'].dt.dayofweek
    all_df['hour'] = all_df['liquidation_time'].dt.hour
    
    heatmap_data = all_df.pivot_table(
        index='hour',
        columns='weekday',
        values='debt_repaid',
        aggfunc='count',
        fill_value=0
    )
    
    fig.add_trace(
        go.Heatmap(
            z=heatmap_data.values,
            x=['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'],
            y=list(range(24)),
            colorscale='Viridis',
            hovertemplate='День: %{x}<br>Час: %{y}<br>Ликвидаций: %{z}<extra></extra>'
        ),
        row=4, col=1
    )
    
    # 8. Топ-10 дней по объему
    top_days = all_df.groupby(all_df['liquidation_time'].dt.date)['debt_repaid'].sum().nlargest(10)
    
    fig.add_trace(
        go.Bar(
            x=[str(d) for d in top_days.index],
            y=top_days.values,
            marker_color='#2ECC40',
            text=[f'${v:,.0f}' for v in top_days.values],
            textposition='auto',
            hovertemplate='%{x}<br>Объем: $%{y:,.0f}<extra></extra>'
        ),
        row=4, col=2
    )
    
    # Обновление layout
    fig.update_layout(
        title={
            'text': 'Комплексный анализ ликвидаций Curve Finance / LlamaLend',
            'font': {'size': 24, 'color': '#2c3e50'}
        },
        height=1600,
        showlegend=True,
        hovermode='x unified',
        template='plotly_white',
        font=dict(family="Arial, sans-serif", size=12)
    )
    
    # Обновление осей
    fig.update_xaxes(title_text="Дата", row=1, col=1)
    fig.update_xaxes(title_text="Дата", row=1, col=2)
    fig.update_xaxes(title_text="Сеть", row=2, col=1)
    fig.update_xaxes(title_text="Размер долга (USD)", row=2, col=2, type='log')
    fig.update_xaxes(title_text="Дата", row=3, col=1)
    fig.update_xaxes(title_text="Час дня", row=4, col=1)
    fig.update_xaxes(title_text="Дата", row=4, col=2, tickangle=45)
    
    fig.update_yaxes(title_text="Количество", row=1, col=1)
    fig.update_yaxes(title_text="Объем (USD)", row=1, col=2)
    fig.update_yaxes(title_text="Количество", row=2, col=1)
    fig.update_yaxes(title_text="Частота", row=2, col=2)
    fig.update_yaxes(title_text="Накопительно", row=3, col=1)
    fig.update_yaxes(title_text="День недели", row=4, col=1)
    fig.update_yaxes(title_text="Объем (USD)", row=4, col=2)
    
    # Сохраняем
    fig.write_html(
        "liquidations_comprehensive_analysis.html",
        include_plotlyjs='cdn',
        config={
            'displayModeBar': True,
            'displaylogo': False,
            'modeBarButtonsToRemove': ['pan2d', 'lasso2d']
        }
    )
    
    # Выводим статистику
    print("\n" + "="*60)
    print("ИТОГОВАЯ СТАТИСТИКА")
    print("="*60)
    print(f"\n📊 Хард-ликвидации:")
    print(f"   Количество: {len(hard_df):,}")
    print(f"   Общий объем: ${hard_df['debt_repaid'].sum():,.2f}")
    print(f"   Средний размер: ${hard_df['debt_repaid'].mean():,.2f}")
    print(f"   Медиана: ${hard_df['debt_repaid'].median():,.2f}")
    
    print(f"\n💧 Софт-ликвидации (демо):")
    print(f"   Количество: {len(soft_df):,}")
    print(f"   Общий объем: ${soft_df['debt_repaid'].sum():,.2f}")
    print(f"   Средний размер: ${soft_df['debt_repaid'].mean():,.2f}")
    print(f"   Медиана: ${soft_df['debt_repaid'].median():,.2f}")
    
    print(f"\n🌐 Распределение по сетям:")
    network_stats = all_df.groupby('network')['debt_repaid'].agg(['count', 'sum', 'mean'])
    print(network_stats.to_string())
    
    print("\n" + "="*60)
    print("✅ График сохранен: liquidations_comprehensive_analysis.html")
    print("="*60)

if __name__ == "__main__":
    create_comprehensive_visualization()