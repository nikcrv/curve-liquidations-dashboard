#!/usr/bin/env python3

import json
import pandas as pd
from datetime import datetime
from collections import defaultdict
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def load_hard_liquidations():
    """Загрузка данных хард-ликвидаций"""
    with open('liquidations_db.json', 'r') as f:
        data = json.load(f)
    
    # Конвертируем в DataFrame
    df = pd.DataFrame(data)
    df['liquidation_time'] = pd.to_datetime(df['liquidation_time'])
    df['type'] = 'Hard Liquidation'
    return df

def load_soft_liquidations():
    """Загрузка данных софт-ликвидаций"""
    soft_data = []
    
    # Попробуем загрузить из файла all_soft_liquidations
    try:
        with open('all_soft_liquidations_20250829_215749.json', 'r') as f:
            all_soft = json.load(f)
            if isinstance(all_soft, dict):
                # Извлекаем все события из всех сетей
                for network, markets in all_soft.items():
                    if isinstance(markets, dict):
                        for market, events in markets.items():
                            if isinstance(events, list):
                                for event in events:
                                    event['network'] = network
                                    event['market'] = market
                                    soft_data.append(event)
    except Exception as e:
        print(f"Ошибка загрузки all_soft_liquidations: {e}")
    
    # Попробуем загрузить из soft_liquidation_users файла
    if not soft_data:
        try:
            with open('soft_liquidation_users_20250829_215127.json', 'r') as f:
                users_data = json.load(f)
                if isinstance(users_data, list):
                    for entry in users_data:
                        # Конвертируем формат пользовательских данных в события
                        event = {
                            'network': entry.get('network', 'unknown'),
                            'user': entry.get('user', ''),
                            'liquidation_time': entry.get('timestamp', entry.get('date', '')),
                            'debt_repaid': entry.get('total_debt', entry.get('debt', 0)),
                            'collateral': entry.get('total_collateral', 0),
                            'tx_hash': entry.get('tx_hash', '')
                        }
                        soft_data.append(event)
        except Exception as e:
            print(f"Ошибка загрузки soft_liquidation_users: {e}")
    
    # Попробуем загрузить из разных файлов
    if not soft_data:
        try:
            with open('soft_liquidations_analysis.json', 'r') as f:
                analysis = json.load(f)
                # Извлекаем события из анализа
                for network_data in analysis.values():
                    if isinstance(network_data, dict) and 'events' in network_data:
                        soft_data.extend(network_data['events'])
        except:
            pass
    
    if not soft_data:
        try:
            with open('soft_liquidation_events.json', 'r') as f:
                events = json.load(f)
                if isinstance(events, dict):
                    for network, data in events.items():
                        if isinstance(data, list):
                            for event in data:
                                event['network'] = network
                                soft_data.append(event)
        except:
            pass
    
    if not soft_data:
        # Создаем пустой DataFrame с правильной структурой
        return pd.DataFrame(columns=['liquidation_time', 'network', 'debt_repaid', 'type'])
    
    df = pd.DataFrame(soft_data)
    
    # Стандартизируем поля
    if 'liquidation_time' not in df.columns and 'timestamp' in df.columns:
        df['liquidation_time'] = df['timestamp']
    if 'liquidation_time' not in df.columns and 'block_timestamp' in df.columns:
        df['liquidation_time'] = df['block_timestamp']
    
    if 'debt_repaid' not in df.columns and 'debt_amount' in df.columns:
        df['debt_repaid'] = df['debt_amount']
    if 'debt_repaid' not in df.columns and 'amount' in df.columns:
        df['debt_repaid'] = df['amount']
    
    # Конвертируем время
    if 'liquidation_time' in df.columns:
        df['liquidation_time'] = pd.to_datetime(df['liquidation_time'], errors='coerce')
    
    df['type'] = 'Soft Liquidation'
    
    return df

def create_comparison_charts():
    """Создание интерактивных графиков сравнения"""
    
    # Загружаем данные
    print("Загрузка данных хард-ликвидаций...")
    hard_df = load_hard_liquidations()
    print(f"Загружено {len(hard_df)} хард-ликвидаций")
    
    print("Загрузка данных софт-ликвидаций...")
    soft_df = load_soft_liquidations()
    print(f"Загружено {len(soft_df)} софт-ликвидаций")
    
    # Объединяем данные
    all_df = pd.concat([hard_df, soft_df], ignore_index=True)
    
    # Фильтруем только валидные даты
    all_df = all_df[all_df['liquidation_time'].notna()]
    
    # Создаем субплоты
    fig = make_subplots(
        rows=3, cols=2,
        subplot_titles=(
            'Количество ликвидаций по времени',
            'Объем ликвидаций по времени (USD)',
            'Распределение по сетям',
            'Средний размер ликвидации',
            'Накопительная статистика',
            'Сравнение по типам'
        ),
        specs=[
            [{'type': 'scatter'}, {'type': 'scatter'}],
            [{'type': 'bar'}, {'type': 'bar'}],
            [{'type': 'scatter'}, {'type': 'pie'}]
        ],
        vertical_spacing=0.12,
        horizontal_spacing=0.15
    )
    
    # 1. График количества ликвидаций по времени
    if not hard_df.empty:
        hard_daily = hard_df.set_index('liquidation_time').resample('D').size()
        fig.add_trace(
            go.Scatter(
                x=hard_daily.index,
                y=hard_daily.values,
                name='Хард-ликвидации',
                line=dict(color='red', width=2),
                hovertemplate='Дата: %{x}<br>Количество: %{y}<extra></extra>'
            ),
            row=1, col=1
        )
    
    if not soft_df.empty:
        soft_daily = soft_df.set_index('liquidation_time').resample('D').size()
        fig.add_trace(
            go.Scatter(
                x=soft_daily.index,
                y=soft_daily.values,
                name='Софт-ликвидации',
                line=dict(color='blue', width=2),
                hovertemplate='Дата: %{x}<br>Количество: %{y}<extra></extra>'
            ),
            row=1, col=1
        )
    
    # 2. График объема ликвидаций по времени
    if 'debt_repaid' in hard_df.columns and not hard_df.empty:
        hard_volume = hard_df.set_index('liquidation_time')['debt_repaid'].resample('D').sum()
        fig.add_trace(
            go.Scatter(
                x=hard_volume.index,
                y=hard_volume.values,
                name='Объем хард',
                line=dict(color='darkred', width=2),
                hovertemplate='Дата: %{x}<br>Объем: ${y:,.0f}<extra></extra>'
            ),
            row=1, col=2
        )
    
    if 'debt_repaid' in soft_df.columns and not soft_df.empty:
        soft_volume = soft_df.set_index('liquidation_time')['debt_repaid'].resample('D').sum()
        fig.add_trace(
            go.Scatter(
                x=soft_volume.index,
                y=soft_volume.values,
                name='Объем софт',
                line=dict(color='darkblue', width=2),
                hovertemplate='Дата: %{x}<br>Объем: ${y:,.0f}<extra></extra>'
            ),
            row=1, col=2
        )
    
    # 3. Распределение по сетям
    if 'network' in all_df.columns:
        network_counts = all_df.groupby(['network', 'type']).size().unstack(fill_value=0)
        
        for liq_type in network_counts.columns:
            color = 'red' if 'Hard' in liq_type else 'blue'
            fig.add_trace(
                go.Bar(
                    x=network_counts.index,
                    y=network_counts[liq_type],
                    name=liq_type,
                    marker_color=color,
                    hovertemplate='Сеть: %{x}<br>Количество: %{y}<extra></extra>'
                ),
                row=2, col=1
            )
    
    # 4. Средний размер ликвидации
    if 'debt_repaid' in all_df.columns and 'network' in all_df.columns:
        avg_size = all_df.groupby(['network', 'type'])['debt_repaid'].mean().unstack(fill_value=0)
        
        for liq_type in avg_size.columns:
            color = 'darkred' if 'Hard' in liq_type else 'darkblue'
            fig.add_trace(
                go.Bar(
                    x=avg_size.index,
                    y=avg_size[liq_type],
                    name=f'Средний {liq_type}',
                    marker_color=color,
                    hovertemplate='Сеть: %{x}<br>Средний размер: ${y:,.0f}<extra></extra>'
                ),
                row=2, col=2
            )
    
    # 5. Накопительная статистика
    if not hard_df.empty:
        hard_cumulative = hard_df.set_index('liquidation_time').resample('D').size().cumsum()
        fig.add_trace(
            go.Scatter(
                x=hard_cumulative.index,
                y=hard_cumulative.values,
                name='Накопительно хард',
                line=dict(color='red', width=2, dash='dash'),
                hovertemplate='Дата: %{x}<br>Всего: %{y}<extra></extra>'
            ),
            row=3, col=1
        )
    
    if not soft_df.empty:
        soft_cumulative = soft_df.set_index('liquidation_time').resample('D').size().cumsum()
        fig.add_trace(
            go.Scatter(
                x=soft_cumulative.index,
                y=soft_cumulative.values,
                name='Накопительно софт',
                line=dict(color='blue', width=2, dash='dash'),
                hovertemplate='Дата: %{x}<br>Всего: %{y}<extra></extra>'
            ),
            row=3, col=1
        )
    
    # 6. Pie chart - сравнение типов
    type_counts = all_df['type'].value_counts()
    fig.add_trace(
        go.Pie(
            labels=type_counts.index,
            values=type_counts.values,
            marker=dict(colors=['red', 'blue']),
            hovertemplate='%{label}<br>Количество: %{value}<br>Процент: %{percent}<extra></extra>'
        ),
        row=3, col=2
    )
    
    # Обновляем layout
    fig.update_layout(
        title_text="Интерактивное сравнение хард и софт ликвидаций Curve/LlamaLend",
        height=1200,
        showlegend=True,
        hovermode='x unified',
        template='plotly_white'
    )
    
    # Обновляем оси
    fig.update_xaxes(title_text="Дата", row=1, col=1)
    fig.update_xaxes(title_text="Дата", row=1, col=2)
    fig.update_xaxes(title_text="Сеть", row=2, col=1)
    fig.update_xaxes(title_text="Сеть", row=2, col=2)
    fig.update_xaxes(title_text="Дата", row=3, col=1)
    
    fig.update_yaxes(title_text="Количество", row=1, col=1)
    fig.update_yaxes(title_text="Объем (USD)", row=1, col=2)
    fig.update_yaxes(title_text="Количество", row=2, col=1)
    fig.update_yaxes(title_text="Средний размер (USD)", row=2, col=2)
    fig.update_yaxes(title_text="Накопительное количество", row=3, col=1)
    
    # Сохраняем в HTML
    fig.write_html(
        "liquidations_comparison_chart.html",
        include_plotlyjs='cdn',
        config={'displayModeBar': True, 'displaylogo': False}
    )
    
    # Создаем сводную статистику
    stats = {
        'total_hard': len(hard_df),
        'total_soft': len(soft_df),
        'hard_volume': hard_df['debt_repaid'].sum() if 'debt_repaid' in hard_df.columns else 0,
        'soft_volume': soft_df['debt_repaid'].sum() if 'debt_repaid' in soft_df.columns else 0,
        'avg_hard': hard_df['debt_repaid'].mean() if 'debt_repaid' in hard_df.columns else 0,
        'avg_soft': soft_df['debt_repaid'].mean() if 'debt_repaid' in soft_df.columns else 0,
        'networks': all_df['network'].unique().tolist() if 'network' in all_df.columns else []
    }
    
    print("\n=== СТАТИСТИКА ===")
    print(f"Хард-ликвидаций: {stats['total_hard']:,}")
    print(f"Софт-ликвидаций: {stats['total_soft']:,}")
    print(f"Общий объем хард: ${stats['hard_volume']:,.2f}")
    print(f"Общий объем софт: ${stats['soft_volume']:,.2f}")
    print(f"Средний размер хард: ${stats['avg_hard']:,.2f}")
    print(f"Средний размер софт: ${stats['avg_soft']:,.2f}")
    print(f"Сети: {', '.join(stats['networks'])}")
    
    return stats

if __name__ == "__main__":
    stats = create_comparison_charts()
    print("\n✅ График сохранен в liquidations_comparison_chart.html")
    print("Откройте файл в браузере для просмотра интерактивного графика")