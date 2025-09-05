#!/usr/bin/env python3

import json
import pandas as pd
from datetime import datetime

def extract_soft_liquidations():
    """Извлекаем софт-ликвидации из структурированных данных"""
    
    # Загружаем данные
    with open('all_soft_liquidations_20250829_215749.json', 'r') as f:
        data = json.load(f)
    
    print("Структура данных:")
    print(f"Ключи верхнего уровня: {list(data.keys())}")
    
    soft_liquidations = []
    
    # Извлекаем данные из lending_markets (это список пользователей)
    if 'lending_markets' in data and isinstance(data['lending_markets'], list):
        print("\nОбработка lending_markets...")
        lending = data['lending_markets']
        
        for user_entry in lending:
            if not isinstance(user_entry, dict):
                continue
                
            # Создаем событие для каждого пользователя в софт-ликвидации
            event = {
                'network': user_entry.get('chain', 'ethereum'),
                'market': user_entry.get('market', 'unknown'),
                'user': user_entry.get('user', user_entry.get('address', '')),
                'liquidation_time': data.get('timestamp', '2025-08-29T21:00:00'),
                'debt_repaid': user_entry.get('debt', 0),
                'collateral': user_entry.get('collateral', 0),
                'bands': user_entry.get('bands', 'N/A'),
                'health': user_entry.get('health', 0),
                'n_loans': user_entry.get('n_loans', 1),
                'type': 'Soft Liquidation',
                'collateral_token': user_entry.get('collateral_token', ''),
                'borrowed_token': user_entry.get('borrowed_token', '')
            }
            soft_liquidations.append(event)
    
    # Извлекаем данные из crvusd_markets (это тоже список)
    if 'crvusd_markets' in data and isinstance(data['crvusd_markets'], list):
        print("Обработка crvusd_markets...")
        crvusd = data['crvusd_markets']
        
        for user_entry in crvusd:
            if not isinstance(user_entry, dict):
                continue
                
            event = {
                'network': user_entry.get('chain', 'ethereum'),
                'market': f"crvUSD-{user_entry.get('market', 'unknown')}",
                'user': user_entry.get('user', user_entry.get('address', '')),
                'liquidation_time': data.get('timestamp', '2025-08-29T21:00:00'),
                'debt_repaid': user_entry.get('debt', 0),
                'collateral': user_entry.get('collateral', 0),
                'bands': user_entry.get('bands', 'N/A'),
                'health': user_entry.get('health', 0),
                'n_loans': user_entry.get('n_loans', 1),
                'type': 'Soft Liquidation',
                'collateral_token': user_entry.get('collateral_token', ''),
                'borrowed_token': 'crvUSD'
            }
            soft_liquidations.append(event)
    
    print(f"\nВсего извлечено софт-ликвидаций: {len(soft_liquidations)}")
    
    # Сохраняем в простом формате
    with open('soft_liquidations_extracted.json', 'w') as f:
        json.dump(soft_liquidations, f, indent=2)
    
    # Статистика
    if soft_liquidations:
        df = pd.DataFrame(soft_liquidations)
        print("\nСтатистика по сетям:")
        print(df['network'].value_counts())
        print(f"\nОбщий объем долга: ${df['debt_repaid'].sum():,.2f}")
        print(f"Средний размер долга: ${df['debt_repaid'].mean():,.2f}")
        print(f"Уникальных пользователей: {df['user'].nunique()}")
        print(f"Уникальных рынков: {df['market'].nunique()}")
    
    return soft_liquidations

if __name__ == "__main__":
    extract_soft_liquidations()
    print("\n✅ Данные сохранены в soft_liquidations_extracted.json")