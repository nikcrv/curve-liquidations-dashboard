#!/usr/bin/env python3
"""
Скрипт для анализа позиций в мягкой ликвидации на Curve Finance
Версия 11: Финальная версия с корректным расчетом TVL

Основные изменения в v11:
- Исправлена проблема с аномальными значениями collateral_usd в БД Metabase
- TVL рассчитывается вручную через price_oracle вместо готовых USD полей
- Для некоторых записей collateral_usd завышен в 10^18-10^25 раз

Переоткрытия:
- Позиция считается новой, если она была переоткрыта (разрыв > 5 часов)
- Каждое переоткрытие считается отдельной позицией

Обработка collateral_up:
- В crvUSD: collateral_up содержит токены, конвертированные в crvUSD при soft liquidation
- В LlamaLend: для некоторых маркетов collateral_up ≈ collateral (разница < 0.01%)
  Это "пыль" (dust) - остатки от LLAMMA, которые не обмениваются для экономии газа.
  В таких случаях используем только collateral для расчета TVL.
"""

import requests
import json
from datetime import datetime, timedelta
import argparse
from collections import defaultdict

# Маппинг chain_id на названия
CHAIN_NAMES = {
    1: 'ETHEREUM',
    10: 'OPTIMISM',
    100: 'GNOSIS',
    137: 'POLYGON',
    250: 'FANTOM',
    324: 'ZKSYNC',
    8453: 'BASE',
    42161: 'ARBITRUM',
    43114: 'AVALANCHE',
    252: 'FRAXTAL',
    146: 'SONIC'
}

class SoftLiquidationAnalyzerWithReopenings:
    def __init__(self, start_date, end_date, session_token=None):
        self.start_date = start_date
        self.end_date = end_date
        self.session_token = session_token or '0b35d466-0c45-4fbd-9927-14e1b850e509'
        self.base_url = 'https://metabase-prices.curve.finance/api/dataset'
        self.headers = {
            'Content-Type': 'application/json',
            'X-Metabase-Session': self.session_token
        }
        self.market_info_cache = {}
        self.token_precision_cache = {}
        self.position_segments = {}  # Для хранения сегментов позиций
    
    def execute_sql(self, query):
        """Execute SQL query on Metabase"""
        payload = {
            'database': 2,
            'type': 'native',
            'native': {'query': query}
        }
        
        try:
            response = requests.post(self.base_url, headers=self.headers, json=payload)
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'rows' in data['data']:
                    return data['data']['rows']
            elif response.status_code == 202:
                # Асинхронный запрос, ждем результат
                data = response.json()
                if 'data' in data and 'rows' in data['data']:
                    return data['data']['rows']
            else:
                print(f"Error: HTTP {response.status_code}")
        except Exception as e:
            print(f"Error executing query: {e}")
        return []
    
    def load_token_precisions(self):
        """Загрузка precision для всех токенов"""
        query = '''
        SELECT DISTINCT
            t.id,
            t.symbol,
            t.precision,
            t.chain_id
        FROM tokens t
        WHERE t.precision IS NOT NULL
        ORDER BY t.symbol, t.chain_id;
        '''
        
        rows = self.execute_sql(query)
        for row in rows:
            token_id = row[0]
            symbol = row[1]
            precision = row[2]
            chain_id = row[3]
            
            self.token_precision_cache[token_id] = float(precision) if precision else 1e18
            key = f"{symbol}_{chain_id}"
            
            # ПРИНУДИТЕЛЬНАЯ коррекция для ARB токена (в БД неправильный precision)
            if symbol == 'ARB' and chain_id == 42161:
                self.token_precision_cache[key] = 1e18  # Правильный precision для ARB
            else:
                self.token_precision_cache[key] = float(precision) if precision else 1e18
    
    def load_crvusd_controllers(self):
        """Загрузка crvUSD контроллеров"""
        
        query = '''
        SELECT DISTINCT
            c.id as controller_id,
            c.chain_id,
            t.symbol as collateral_token,
            t.name as token_name,
            t.precision
        FROM crvusd__controllers c
        LEFT JOIN tokens t ON t.id = c.collateral_token_id
        ORDER BY controller_id;
        '''
        
        rows = self.execute_sql(query)
        for row in rows:
            controller_id = row[0]
            chain_id = row[1]
            collateral_token = row[2] or 'Unknown'
            token_name = row[3] or 'Unknown'
            precision = float(row[4]) if row[4] else 1e18
            
            # Применяем коррекции precision как для lending маркетов
            if collateral_token == 'ARB' and chain_id == 42161:
                precision = 1e18
            
            self.market_info_cache[f'crvusd_{controller_id}'] = {
                'chain_id': chain_id,
                'chain_name': CHAIN_NAMES.get(chain_id, f'Chain-{chain_id}'),
                'market_name': f'{collateral_token}-crvUSD',
                'collateral_token': collateral_token,
                'token_symbol': collateral_token,
                'precision': precision
            }
    
    def load_market_mappings(self):
        """Загрузка маппинга рынков"""
        
        self.load_token_precisions()
        self.load_crvusd_controllers()
        
        # Загружаем маппинг для lending markets
        query = '''
        SELECT 
            lm.id as market_id,
            lm.chain_id,
            lm.name as market_name,
            CASE 
                WHEN lm.name LIKE '%WBTC%' OR lm.name LIKE '%wBTC%' THEN 'WBTC'
                WHEN lm.name LIKE '%cbBTC%' THEN 'cbBTC'
                WHEN lm.name LIKE '%LBTC%' THEN 'LBTC'
                WHEN lm.name LIKE '%tBTC%' THEN 'tBTC'
                WHEN lm.name LIKE '%wstETH%' THEN 'wstETH'
                WHEN lm.name LIKE '%WETH%' THEN 'WETH'
                WHEN lm.name LIKE '%ETH%' AND lm.name NOT LIKE '%wstETH%' THEN 'ETH'
                WHEN lm.name LIKE '%CRV%' THEN 'CRV'
                WHEN lm.name LIKE '%ARB%' THEN 'ARB'
                WHEN lm.name LIKE '%sUSDe%' THEN 'sUSDe'
                WHEN lm.name LIKE '%sfrxETH%' THEN 'sfrxETH'
                WHEN lm.name LIKE '%sfrxUSD%' THEN 'sfrxUSD'
                WHEN lm.name LIKE '%sFRAX%' THEN 'sFRAX'
                WHEN lm.name LIKE '%FXS%' THEN 'FXS'
                WHEN lm.name LIKE '%FXN%' THEN 'FXN'
                WHEN lm.name LIKE '%SQUID%' THEN 'SQUID'
                WHEN lm.name LIKE '%EYWA%' THEN 'EYWA'
                WHEN lm.name LIKE '%OP%' THEN 'OP'
                WHEN lm.name LIKE '%UWU%' THEN 'UWU'
                WHEN lm.name LIKE '%weETH%' THEN 'weETH'
                ELSE SPLIT_PART(lm.name, '-', 1)
            END as collateral_token
        FROM lending__markets lm
        ORDER BY lm.id;
        '''
        
        rows = self.execute_sql(query)
        print(f"  ✓ Загружено {len(rows)} маркетов LlamaLend")
        for row in rows:
            market_id = row[0]
            chain_id = row[1]
            market_name = row[2]
            token = row[3]
            
            precision_key = f"{token}_{chain_id}"
            precision = self.token_precision_cache.get(precision_key, 1e18)
            
            self.market_info_cache[f'lending_{market_id}'] = {
                'chain_id': chain_id,
                'chain_name': CHAIN_NAMES.get(chain_id, f'Chain-{chain_id}'),
                'market_name': market_name,
                'collateral_token': token,
                'token_symbol': token,
                'precision': precision
            }
        
        # Загружаем маппинг для crvUSD controllers
        query = '''
        SELECT DISTINCT
            c.id as controller_id,
            c.chain_id,
            t.symbol as collateral_token,
            t.name as token_name,
            t.precision
        FROM crvusd__controllers c
        LEFT JOIN tokens t ON t.id = c.collateral_token_id
        ORDER BY controller_id;
        '''
        
        rows = self.execute_sql(query)
        for row in rows:
            controller_id = row[0]
            chain_id = row[1]
            token = row[2]
            precision = float(row[4]) if row[4] else 1e18
            
            self.market_info_cache[f'crvusd_{controller_id}'] = {
                'chain_id': chain_id,
                'chain_name': CHAIN_NAMES.get(chain_id, f'Chain-{chain_id}'),
                'market_name': f'crvUSD-{token}',
                'collateral_token': token,
                'token_symbol': token,
                'precision': precision
            }
    
    def get_token_precision(self, symbol, chain_id):
        """Получение precision для токена"""
        key = f"{symbol}_{chain_id}"
        if key in self.token_precision_cache:
            return self.token_precision_cache[key]
        
        # Fallback значения - используем ПРАВИЛЬНЫЕ precision для токенов
        # даже если в базе данных записано неверно
        precision_map = {
            'WBTC': 1e8, 'cbBTC': 1e8, 'tBTC': 1e18,
            'USDC': 1e6, 'USDT': 1e6, 'crvUSD': 1e18,
            'WETH': 1e18, 'sUSDe': 1e18, 'USDe': 1e18,
            'ARB': 1e18  # ARB token has 18 decimals on Arbitrum (0x912ce59144191c1204e64559fe8253a0e49e6548)
        }
        
        # ПРИНУДИТЕЛЬНАЯ коррекция для ARB токена
        if symbol == 'ARB' and chain_id == 42161:
            return 1e18
        
        return precision_map.get(symbol, 1e18)
    
    def detect_position_segments(self, market_id, user):
        """Обнаружение сегментов позиции (переоткрытий) для пользователя на маркете"""
        
        # Расширяем период поиска, чтобы учесть переоткрытия до начальной даты
        extended_start = datetime.strptime(self.start_date, '%Y-%m-%d') - timedelta(days=180)
        extended_start_str = extended_start.strftime('%Y-%m-%d')
        
        query = f'''
        SELECT 
            dt,
            debt
        FROM lending__user_snapshot
        WHERE market_id = {market_id}
            AND "user" = '{user}'
            AND dt >= TIMESTAMP '{extended_start_str}'
            AND dt < TIMESTAMP '{self.end_date}'
        ORDER BY dt
        '''
        
        rows = self.execute_sql(query)
        if len(rows) < 2:
            return [{'start': None, 'end': None, 'segment_id': 1}]
        
        segments = []
        segment_start = None
        segment_id = 1
        
        for i, row in enumerate(rows):
            dt_str = row[0]
            debt = float(row[1]) if row[1] else 0
            
            dt = datetime.fromisoformat(dt_str.replace('Z', ''))
            
            if debt > 0 and segment_start is None:
                # Начало нового сегмента
                segment_start = dt
            elif debt == 0 and segment_start is not None:
                # Конец сегмента
                segments.append({
                    'start': segment_start,
                    'end': dt,
                    'segment_id': segment_id
                })
                segment_start = None
                segment_id += 1
            elif i > 0 and segment_start is not None:
                # Проверяем разрыв между записями
                prev_dt = datetime.fromisoformat(rows[i-1][0].replace('Z', ''))
                time_diff = dt - prev_dt
                hours_diff = time_diff.total_seconds() / 3600
                
                if hours_diff > 5:  # Разрыв > 5 часов = переоткрытие
                    # Закрываем текущий сегмент
                    segments.append({
                        'start': segment_start,
                        'end': prev_dt,
                        'segment_id': segment_id
                    })
                    # Начинаем новый сегмент
                    segment_start = dt
                    segment_id += 1
        
        # Если позиция все еще открыта
        if segment_start is not None:
            segments.append({
                'start': segment_start,
                'end': None,
                'segment_id': segment_id
            })
        
        return segments if segments else [{'start': None, 'end': None, 'segment_id': 1}]
    
    def analyze_positions(self):
        """Анализ позиций с учетом переоткрытий"""
        print("🔍 Загрузка маппинга рынков и токенов...")
        self.load_market_mappings()
        
        print(f"📊 Анализ мягких ликвидаций с {self.start_date} по {self.end_date}")
        print("🎯 Учитываем переоткрытия позиций (разрыв > 5 часов)")
        
        # Получаем всех пользователей, у которых была мягкая ликвидация
        query = f'''
        SELECT DISTINCT
            market_id,
            "user"
        FROM lending__user_snapshot
        WHERE dt >= TIMESTAMP '{self.start_date}'
            AND dt < TIMESTAMP '{self.end_date}'
            AND soft_liquidation = true
            AND debt > 0
        ORDER BY market_id, "user"
        '''
        
        user_market_pairs = self.execute_sql(query)
        print(f"📈 Найдено {len(user_market_pairs)} уникальных комбинаций (пользователь + маркет) с мягкими ликвидациями")
        if len(user_market_pairs) > 0:
            print(f"  Пример: market_id={user_market_pairs[0][0]}, user={user_market_pairs[0][1][:20]}...")
        
        all_positions = []
        total_segments = 0
        
        # Для каждой пары пользователь-маркет определяем сегменты позиций
        for market_id, user in user_market_pairs:
            segments = self.detect_position_segments(market_id, user)
            total_segments += len(segments)
            
            for segment in segments:
                # Для каждого сегмента получаем данные по мягким ликвидациям
                segment_data = self.get_position_data(market_id, user, segment)
                if segment_data:
                    all_positions.append(segment_data)
        
        print(f"🔄 Обнаружено {total_segments} сегментов позиций (включая переоткрытия)")
        print(f"💰 Из них {len(all_positions)} сегментов с мягкими ликвидациями в анализируемом периоде")
        
        # Добавляем анализ crvUSD позиций
        crvusd_positions = self.analyze_crvusd_positions()
        all_positions.extend(crvusd_positions)
        
        print(f"📋 Общее количество сегментов (LlamaLend + crvUSD): {len(all_positions)}")
        
        return all_positions
    
    def analyze_crvusd_positions(self):
        """Анализ crvUSD позиций с учетом переоткрытий"""
        print("🔍 Анализ crvUSD позиций...")
        
        # Получаем всех пользователей crvUSD с мягкими ликвидациями
        query = f'''
        SELECT DISTINCT
            controller_id,
            "user"
        FROM crvusd__user_snapshot
        WHERE dt >= TIMESTAMP '{self.start_date}'
            AND dt < TIMESTAMP '{self.end_date}'
            AND soft_liquidation = true
            AND debt > 0
        ORDER BY controller_id, "user"
        '''
        
        crvusd_pairs = self.execute_sql(query)
        print(f"📈 Найдено {len(crvusd_pairs)} уникальных crvUSD комбинаций (пользователь + контроллер)")
        
        all_crvusd_positions = []
        total_segments = 0
        
        # Для каждой пары пользователь-контроллер определяем сегменты позиций
        for controller_id, user in crvusd_pairs:
            segments = self.detect_crvusd_segments(controller_id, user)
            total_segments += len(segments)
            
            for segment in segments:
                # Для каждого сегмента получаем данные по мягким ликвидациям
                segment_data = self.get_crvusd_data(controller_id, user, segment)
                if segment_data:
                    all_crvusd_positions.append(segment_data)
        
        print(f"🔄 crvUSD: {total_segments} сегментов, {len(all_crvusd_positions)} с мягкими ликвидациями")
        
        return all_crvusd_positions
    
    def detect_crvusd_segments(self, controller_id, user):
        """Обнаружение сегментов crvUSD позиций (переоткрытий)"""
        
        # Расширяем период поиска для учета переоткрытий до начальной даты
        extended_start = datetime.strptime(self.start_date, '%Y-%m-%d') - timedelta(days=180)
        extended_start_str = extended_start.strftime('%Y-%m-%d')
        
        query = f'''
        SELECT 
            dt,
            debt
        FROM crvusd__user_snapshot
        WHERE controller_id = {controller_id}
            AND "user" = '{user}'
            AND dt >= TIMESTAMP '{extended_start_str}'
            AND dt < TIMESTAMP '{self.end_date}'
        ORDER BY dt
        '''
        
        rows = self.execute_sql(query)
        if len(rows) < 2:
            return [{'start': None, 'end': None, 'segment_id': 1}]
        
        segments = []
        segment_start = None
        segment_id = 1
        
        for i, row in enumerate(rows):
            dt_str = row[0]
            debt = float(row[1]) if row[1] else 0
            
            dt = datetime.fromisoformat(dt_str.replace('Z', ''))
            
            if debt > 0 and segment_start is None:
                # Начало нового сегмента
                segment_start = dt
            elif debt == 0 and segment_start is not None:
                # Конец сегмента
                segments.append({
                    'start': segment_start,
                    'end': dt,
                    'segment_id': segment_id
                })
                segment_start = None
                segment_id += 1
            elif i > 0 and debt > 0 and segment_start is not None:
                # Проверяем разрыв между записями
                prev_dt_str = rows[i-1][0]
                prev_dt = datetime.fromisoformat(prev_dt_str.replace('Z', ''))
                hours_diff = (dt - prev_dt).total_seconds() / 3600
                
                if hours_diff > 5:  # Разрыв больше 5 часов = переоткрытие
                    # Завершаем текущий сегмент
                    segments.append({
                        'start': segment_start,
                        'end': prev_dt,
                        'segment_id': segment_id
                    })
                    # Начинаем новый сегмент
                    segment_start = dt
                    segment_id += 1
        
        # Если позиция все еще активна в конце периода
        if segment_start is not None:
            segments.append({
                'start': segment_start,
                'end': None,
                'segment_id': segment_id
            })
        
        return segments if segments else [{'start': None, 'end': None, 'segment_id': 1}]
    
    def get_crvusd_data(self, controller_id, user, segment):
        """Получение данных для сегмента crvUSD позиции"""
        segment_condition = ""
        if segment['start'] and segment['end']:
            segment_condition = f"AND dt >= TIMESTAMP '{segment['start'].isoformat()}' AND dt <= TIMESTAMP '{segment['end'].isoformat()}'"
        elif segment['start']:
            segment_condition = f"AND dt >= TIMESTAMP '{segment['start'].isoformat()}'"
        
        query = f'''
        SELECT 
            controller_id,
            "user",
            MIN(dt) as first_sl,
            MAX(dt) as last_sl,
            MAX(CASE WHEN soft_liquidation = true THEN collateral END) as max_collateral_raw,
            MAX(CASE WHEN soft_liquidation = true THEN collateral_up END) as max_collateral_up_raw,
            MAX(CASE WHEN soft_liquidation = true THEN debt END) as max_debt_raw,
            AVG(CASE WHEN soft_liquidation = true AND price_oracle > 0 THEN price_oracle / 1e18 END) as token_price,
            COUNT(DISTINCT DATE(dt)) as days_in_sl
        FROM crvusd__user_snapshot
        WHERE controller_id = {controller_id}
            AND "user" = '{user}'
            AND dt >= TIMESTAMP '{self.start_date}'
            AND dt < TIMESTAMP '{self.end_date}'
            AND soft_liquidation = true
            {segment_condition}
        GROUP BY controller_id, "user"
        '''
        
        rows = self.execute_sql(query)
        if not rows or not rows[0][4]:  # Нет данных или нет collateral
            return None
        
        row = rows[0]
        controller_id = row[0]
        user = row[1]
        first_sl = row[2]
        last_sl = row[3]
        max_collateral_raw = float(row[4]) if row[4] else 0
        max_collateral_up_raw = float(row[5]) if row[5] else 0
        max_debt_raw = float(row[6]) if row[6] else 0
        token_price = float(row[7]) if row[7] else 0
        days_in_sl = row[8] if row[8] else 0
        
        # Ищем информацию о контроллере
        market_info = self.market_info_cache.get(f'crvusd_{controller_id}', {})
        
        # Используем правильный precision
        token_symbol = market_info.get('token_symbol', 'UNKNOWN')
        chain_id = market_info.get('chain_id', 1)
        precision = self.get_token_precision(token_symbol, chain_id)
        
        # Расчет TVL для crvUSD: collateral + collateral_up - оба в токенах
        collateral_normalized = max_collateral_raw / precision if precision else max_collateral_raw
        collateral_up_normalized = max_collateral_up_raw / precision if precision else max_collateral_up_raw
        total_collateral = collateral_normalized + collateral_up_normalized
        debt_normalized = max_debt_raw / 1e18  # debt всегда в crvUSD с precision 1e18
        collateral_usd = total_collateral * token_price if token_price else 0
        
        return {
            'market_id': controller_id,
            'user': user,
            'segment_id': segment['segment_id'],
            'chain_id': market_info.get('chain_id'),
            'chain_name': market_info.get('chain_name', 'UNKNOWN'),
            'market_name': market_info.get('market_name', f'Controller-{controller_id}'),
            'token_symbol': token_symbol,
            'first_sl': first_sl,
            'last_sl': last_sl,
            'max_collateral': total_collateral,  # Для crvUSD это сумма collateral + collateral_up
            'max_debt': debt_normalized,
            'max_collateral_usd': collateral_usd,
            'days_in_sl': days_in_sl,
            'precision': precision,
            'platform': 'crvUSD'  # Маркер что это crvUSD позиция
        }
    
    def get_position_data(self, market_id, user, segment):
        """Получение данных для сегмента позиции"""
        segment_condition = ""
        if segment['start'] and segment['end']:
            segment_condition = f"AND dt >= TIMESTAMP '{segment['start'].isoformat()}' AND dt <= TIMESTAMP '{segment['end'].isoformat()}'"
        elif segment['start']:
            segment_condition = f"AND dt >= TIMESTAMP '{segment['start'].isoformat()}'"
        
        query = f'''
        SELECT 
            market_id,
            "user",
            MIN(dt) as first_sl,
            MAX(dt) as last_sl,
            MAX(CASE WHEN soft_liquidation = true THEN collateral END) as max_collateral_raw,
            MAX(CASE WHEN soft_liquidation = true THEN debt END) as max_debt_raw,
            AVG(CASE WHEN soft_liquidation = true AND price_oracle > 0 THEN price_oracle / 1e18 END) as token_price,
            MAX(CASE WHEN soft_liquidation = true THEN collateral_up END) as max_collateral_up_raw,
            COUNT(DISTINCT DATE(dt)) as days_in_sl
        FROM lending__user_snapshot
        WHERE market_id = {market_id}
            AND "user" = '{user}'
            AND dt >= TIMESTAMP '{self.start_date}'
            AND dt < TIMESTAMP '{self.end_date}'
            AND soft_liquidation = true
            {segment_condition}
        GROUP BY market_id, "user"
        HAVING MAX(CASE WHEN soft_liquidation = true THEN debt END) > 0
        '''
        
        rows = self.execute_sql(query)
        
        if not rows:
            return None
        
        row = rows[0]
        market_id = row[0]
        user = row[1]
        first_sl = row[2]
        last_sl = row[3]
        max_collateral_raw = float(row[4]) if row[4] else 0
        max_debt_raw = float(row[5]) if row[5] else 0
        token_price = float(row[6]) if row[6] else 0
        max_collateral_up_raw = float(row[7]) if row[7] else 0
        days_in_sl = row[8] if row[8] else 0
        
        # Ищем информацию о маркете (может быть lending или crvusd)
        market_info = self.market_info_cache.get(f'lending_{market_id}') or self.market_info_cache.get(f'crvusd_{market_id}') or {}
        
        # Используем правильный precision из get_token_precision вместо кэша
        token_symbol = market_info.get('token_symbol', 'UNKNOWN')
        chain_id = market_info.get('chain_id', 1)
        precision = self.get_token_precision(token_symbol, chain_id)
        
        # Поскольку token_price уже нормализован в SQL (price_oracle / 1e18),
        # нужно нормализовать только collateral по precision токена
        collateral_normalized = max_collateral_raw / precision if precision else max_collateral_raw
        debt_normalized = max_debt_raw / precision if precision else max_debt_raw
        
        # Расчет базового TVL от collateral
        collateral_token_usd = collateral_normalized * token_price if token_price else 0
        
        # Обработка collateral_up для LlamaLend
        # Проверяем, является ли collateral_up "пылью" (dust) от LLAMMA
        if max_collateral_up_raw > 0 and max_collateral_raw > 0:
            # Вычисляем абсолютную и относительную разницу
            abs_diff = abs(max_collateral_up_raw - max_collateral_raw)
            rel_diff = abs_diff / max_collateral_raw
            
            # Если разница < 0.01% (0.0001), это "пыль" - остатки от soft liquidation
            if rel_diff < 0.0001:  # Порог для определения "пыли"
                # collateral_up содержит почти то же, что и collateral (± несколько wei)
                # Используем только collateral для TVL, чтобы избежать дублирования
                collateral_usd = collateral_token_usd
            else:
                # Значительная разница - collateral_up содержит реальное значение
                # В LlamaLend это должна быть USD стоимость заёмного актива
                collateral_up_usd = max_collateral_up_raw / 1e18
                collateral_usd = collateral_token_usd + collateral_up_usd
        else:
            # Стандартная обработка когда collateral_up = 0 или collateral = 0
            collateral_up_usd = max_collateral_up_raw / 1e18 if max_collateral_up_raw else 0
            collateral_usd = collateral_token_usd + collateral_up_usd

        return {
            'market_id': market_id,
            'user': user,
            'segment_id': segment['segment_id'],
            'chain_id': market_info.get('chain_id'),
            'chain_name': market_info.get('chain_name', 'UNKNOWN'),
            'market_name': market_info.get('market_name', 'Unknown Market'),
            'token_symbol': market_info.get('token_symbol', 'UNKNOWN'),
            'first_sl': first_sl,
            'last_sl': last_sl,
            'max_collateral': collateral_normalized,
            'max_debt': debt_normalized,
            'max_collateral_usd': collateral_usd,
            'days_in_sl': days_in_sl,
            'precision': precision,
            'platform': 'LlamaLend'  # Маркер что это LlamaLend позиция
        }
    
    def generate_report(self, positions):
        """Генерация отчета с учетом переоткрытий"""
        if not positions:
            print("❌ Позиции в мягкой ликвидации не найдены")
            return
        
        print(f"\n📊 ОТЧЕТ ПО МЯГКИМ ЛИКВИДАЦИЯМ ({self.start_date} - {self.end_date})")
        print("=" * 80)
        
        total_positions = len(positions)
        total_tvl = sum(pos['max_collateral_usd'] for pos in positions)
        unique_users = len(set(pos['user'] for pos in positions))
        unique_user_market_pairs = len(set((pos['user'], pos['market_id']) for pos in positions))
        
        # Считаем переоткрытия
        reopenings_count = sum(1 for pos in positions if pos['segment_id'] > 1)
        
        print(f"🎯 ОБЩАЯ СТАТИСТИКА:")
        print(f"  Уникальных пользователей: {unique_users:,}")
        print(f"  Уникальных позиций (пользователь + маркет): {unique_user_market_pairs:,}")
        print(f"  Всего сегментов позиций: {total_positions:,}")
        print(f"  Из них переоткрытий: {reopenings_count:,} ({reopenings_count/total_positions*100:.1f}%)")
        print(f"  Общий TVL: ${total_tvl:,.2f}")
        print(f"  Средний TVL на сегмент: ${total_tvl/total_positions:,.2f}")
        
        # Группировка по платформам, сетям и маркетам
        by_platform_chain_market = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {'count': 0, 'tvl': 0, 'reopenings': 0, 'token': '', 'name': ''})))
        
        for pos in positions:
            platform = pos.get('platform', 'LlamaLend')
            chain = pos['chain_name']
            market_id = pos['market_id']
            market_name = pos.get('market_name', f'Market-{market_id}')
            token = pos.get('token_symbol', 'UNKNOWN')
            
            by_platform_chain_market[platform][chain][market_id]['count'] += 1
            by_platform_chain_market[platform][chain][market_id]['tvl'] += pos['max_collateral_usd']
            by_platform_chain_market[platform][chain][market_id]['token'] = token
            by_platform_chain_market[platform][chain][market_id]['name'] = market_name
            if pos['segment_id'] > 1:
                by_platform_chain_market[platform][chain][market_id]['reopenings'] += 1
        
        print(f"\n📊 РАСПРЕДЕЛЕНИЕ ПО ПЛАТФОРМАМ, СЕТЯМ И МАРКЕТАМ:")
        
        for platform in sorted(by_platform_chain_market.keys()):
            platform_total_count = 0
            platform_total_tvl = 0
            platform_total_reopenings = 0
            
            for chain in by_platform_chain_market[platform]:
                for market_id in by_platform_chain_market[platform][chain]:
                    data = by_platform_chain_market[platform][chain][market_id]
                    platform_total_count += data['count']
                    platform_total_tvl += data['tvl']
                    platform_total_reopenings += data['reopenings']
            
            reopening_pct = platform_total_reopenings/platform_total_count*100 if platform_total_count > 0 else 0
            print(f"\n🏦 {platform}:")
            print(f"  Всего: {platform_total_count} сегментов, ${platform_total_tvl:,.2f} TVL")
            print(f"  Переоткрытий: {platform_total_reopenings} ({reopening_pct:.1f}%)")
            
            # Группировка по сетям внутри платформы
            by_chain = defaultdict(lambda: {'count': 0, 'tvl': 0, 'reopenings': 0, 'markets': {}})
            
            for chain in by_platform_chain_market[platform]:
                for market_id in by_platform_chain_market[platform][chain]:
                    data = by_platform_chain_market[platform][chain][market_id]
                    by_chain[chain]['count'] += data['count']
                    by_chain[chain]['tvl'] += data['tvl']
                    by_chain[chain]['reopenings'] += data['reopenings']
                    by_chain[chain]['markets'][market_id] = data
            
            for chain, chain_data in sorted(by_chain.items(), key=lambda x: x[1]['tvl'], reverse=True):
                chain_reopening_pct = chain_data['reopenings']/chain_data['count']*100 if chain_data['count'] > 0 else 0
                print(f"\n  🌐 {chain}:")
                print(f"    Всего: {chain_data['count']} сегментов, ${chain_data['tvl']:,.2f} TVL")
                print(f"    Переоткрытий: {chain_data['reopenings']} ({chain_reopening_pct:.1f}%)")
                
                # Маркеты внутри сети
                print(f"    Маркеты:")
                for market_id, market_data in sorted(chain_data['markets'].items(), key=lambda x: x[1]['tvl'], reverse=True):
                    market_reopening_pct = market_data['reopenings']/market_data['count']*100 if market_data['count'] > 0 else 0
                    market_name = market_data['name'][:20]  # Ограничиваем длину названия
                    token = market_data['token']
                    print(f"      • [{market_id:3}] {market_name:20} ({token:8}): {market_data['count']:3} сегментов, ${market_data['tvl']:12,.2f} TVL, {market_data['reopenings']:2} переоткрытий ({market_reopening_pct:5.1f}%)")
        
        # ТОП-10 позиций по TVL
        top_positions = sorted(positions, key=lambda x: x['max_collateral_usd'], reverse=True)[:10]
        print(f"\n🏆 ТОП-10 СЕГМЕНТОВ ПО TVL:")
        for i, pos in enumerate(top_positions, 1):
            reopening_marker = f" (переоткрытие #{pos['segment_id']})" if pos['segment_id'] > 1 else ""
            platform = pos.get('platform', 'LlamaLend')
            print(f"  {i}. ${pos['max_collateral_usd']:,.2f} - {pos['token_symbol']} на {pos['chain_name']} ({platform})")
            print(f"     User: {pos['user'][:10]}...{reopening_marker}")
        
        # Анализ переоткрытий
        if reopenings_count > 0:
            reopened_positions = [pos for pos in positions if pos['segment_id'] > 1]
            
            print(f"\n🔄 АНАЛИЗ ПЕРЕОТКРЫТИЙ:")
            print(f"  Всего переоткрытых сегментов: {reopenings_count}")
            
            # Группируем по пользователям
            users_with_reopenings = defaultdict(list)
            for pos in reopened_positions:
                users_with_reopenings[pos['user']].append(pos)
            
            print(f"  Пользователей с переоткрытиями: {len(users_with_reopenings)}")
            
            # Самые активные пользователи переоткрытий
            top_reopeners = sorted(users_with_reopenings.items(), key=lambda x: len(x[1]), reverse=True)[:5]
            print(f"  ТОП-5 по количеству переоткрытий:")
            for user, user_positions in top_reopeners:
                print(f"    {user[:10]}...: {len(user_positions)} переоткрытий")

def main():
    parser = argparse.ArgumentParser(description='Анализ мягких ликвидаций с учетом переоткрытий')
    parser.add_argument('--start', required=True, help='Дата начала (YYYY-MM-DD)')
    parser.add_argument('--end', required=True, help='Дата окончания (YYYY-MM-DD)')
    parser.add_argument('--token', help='Токен сессии Metabase')
    
    args = parser.parse_args()
    
    analyzer = SoftLiquidationAnalyzerWithReopenings(
        start_date=args.start,
        end_date=args.end,
        session_token=args.token
    )
    
    positions = analyzer.analyze_positions()
    analyzer.generate_report(positions)

if __name__ == "__main__":
    main()