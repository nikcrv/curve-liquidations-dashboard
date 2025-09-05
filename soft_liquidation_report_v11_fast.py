#!/usr/bin/env python3
"""
Скрипт для анализа позиций в мягкой ликвидации на Curve Finance
Версия 11 FAST: Оптимизированная версия без сегментации позиций
"""

import requests
import json
import os
from datetime import datetime, timedelta
import argparse
from collections import defaultdict

# Маппинг chain_id на названия
CHAIN_NAMES = {}

class SoftLiquidationAnalyzer:
    def __init__(self, start_date, end_date, session_token=None):
        self.start_date = start_date
        self.end_date = end_date
        if not session_token:
            import os
            session_token = os.getenv('METABASE_SESSION_TOKEN', '0b35d466-0c45-4fbd-9927-14e1b850e509')
        self.session_token = session_token
        self.base_url = 'https://metabase-prices.curve.finance/api/dataset'
        self.headers = {
            'Content-Type': 'application/json',
            'X-Metabase-Session': self.session_token
        }
        self.market_info_cache = {}
        self.token_precision_cache = {}
    
    def execute_sql(self, query):
        """Execute SQL query on Metabase"""
        database_id = int(os.getenv('METABASE_DATABASE_ID', '2'))
        payload = {
            'database': database_id,
            'type': 'native',
            'native': {'query': query}
        }
        
        try:
            response = requests.post(self.base_url, headers=self.headers, json=payload)
            if response.status_code in [200, 202]:
                data = response.json()
                if 'data' in data and 'rows' in data['data']:
                    return data['data']['rows']
            else:
                print(f"Error: HTTP {response.status_code}")
        except Exception as e:
            print(f"Error executing query: {e}")
        return []
    
    def load_chain_names(self):
        """Загрузка названий сетей из БД"""
        global CHAIN_NAMES
        
        # Пытаемся загрузить из таблицы chains
        query = '''
        SELECT id, name
        FROM chains
        ORDER BY id;
        '''
        
        try:
            rows = self.execute_sql(query)
            if rows:
                for row in rows:
                    chain_id = row[0]
                    chain_name = row[1].upper() if row[1] else f'CHAIN-{chain_id}'
                    CHAIN_NAMES[chain_id] = chain_name
                print(f"  ✓ Загружено {len(CHAIN_NAMES)} названий сетей из таблицы chains")
                return
        except:
            pass
        
        # Если нет таблицы chains, используем стандартные значения
        CHAIN_NAMES[1] = 'ETHEREUM'
        CHAIN_NAMES[10] = 'OPTIMISM'
        CHAIN_NAMES[42161] = 'ARBITRUM'
        CHAIN_NAMES[252] = 'FRAXTAL'
        print(f"  ✓ Используем стандартные названия сетей")
    
    def load_token_precisions(self):
        """Загрузка precision для всех токенов через правильную связь с lending__controllers"""
        print("  ⏳ Загрузка precision для токенов...")
        
        # Загружаем precision для LlamaLend маркетов через связь market_id
        query = '''
        SELECT DISTINCT
            lm.id as market_id,
            lm.name as market_name,
            lm.chain_id,
            SPLIT_PART(lm.name, '-', 1) as token_symbol,
            t.symbol as real_symbol,
            t.precision,
            t.address,
            t2.symbol as borrowed_symbol,
            t2.precision as borrowed_precision
        FROM lending__markets lm
        LEFT JOIN lending__controllers lc ON lc.market_id = lm.id
        LEFT JOIN tokens t ON t.id = lc.collateral_token_id
        LEFT JOIN tokens t2 ON t2.id = lc.borrowed_token_id
        WHERE lc.collateral_token_id IS NOT NULL;
        '''
        
        rows = self.execute_sql(query)
        self.market_token_map = {}  # Маппинг market_id -> precision
        self.market_borrowed_map = {}  # Маппинг market_id -> borrowed token precision
        
        for row in rows:
            market_id = row[0]
            market_name = row[1]
            chain_id = row[2]
            parsed_symbol = row[3]
            real_symbol = row[4]
            precision = row[5]
            address = row[6]
            borrowed_symbol = row[7]
            borrowed_precision = row[8]
            
            # Сохраняем precision для market_id
            self.market_token_map[market_id] = {
                'symbol': real_symbol or parsed_symbol,
                'precision': float(precision) if precision else 1e18,
                'address': address
            }
            
            # Сохраняем borrowed token precision
            self.market_borrowed_map[market_id] = {
                'symbol': borrowed_symbol,
                'precision': float(borrowed_precision) if borrowed_precision else 1e18
            }
            
            # Также сохраняем в общий кэш по символу
            key = f"{real_symbol or parsed_symbol}_{chain_id}"
            if precision:
                self.token_precision_cache[key] = float(precision)
        
        # Загружаем precision для crvUSD controllers
        query = '''
        SELECT DISTINCT
            c.id as controller_id,
            c.chain_id,
            t.symbol,
            t.precision,
            t.address
        FROM crvusd__controllers c
        LEFT JOIN tokens t ON t.id = c.collateral_token_id
        WHERE c.collateral_token_id IS NOT NULL;
        '''
        
        rows = self.execute_sql(query)
        self.controller_token_map = {}  # Маппинг controller_id -> precision
        
        for row in rows:
            controller_id = row[0]
            chain_id = row[1]
            symbol = row[2]
            precision = row[3]
            address = row[4]
            
            # Сохраняем precision для controller_id
            self.controller_token_map[controller_id] = {
                'symbol': symbol,
                'precision': float(precision) if precision else 1e18,
                'address': address
            }
            
            # Также сохраняем в общий кэш
            key = f"{symbol}_{chain_id}"
            if precision:
                self.token_precision_cache[key] = float(precision)
        
        print(f"  ✓ Загружено {len(self.market_token_map)} LlamaLend маркетов")
        print(f"  ✓ Загружено {len(self.controller_token_map)} crvUSD контроллеров")
        print(f"  ✓ Всего {len(self.token_precision_cache)} precision значений")
    
    def get_token_precision(self, symbol, chain_id):
        """Получение precision из кэша"""
        key = f"{symbol}_{chain_id}"
        return self.token_precision_cache.get(key, 1e18)
    
    def analyze_positions_with_segments(self):
        """Анализ позиций с учетом сегментации (переоткрытий)"""
        print("🔍 Загрузка данных...")
        self.load_chain_names()
        self.load_token_precisions()
        
        print(f"📊 Анализ мягких ликвидаций с {self.start_date} по {self.end_date}")
        print("🎯 Учитываем переоткрытия позиций (разрыв > 5 часов)")
        
        # Расширяем период для поиска сегментов
        from datetime import datetime, timedelta
        extended_start = datetime.strptime(self.start_date, '%Y-%m-%d') - timedelta(days=30)
        extended_start_str = extended_start.strftime('%Y-%m-%d')
        
        # Получаем данные с историей для определения сегментов
        query = f'''
        WITH position_data AS (
            SELECT 
                lus.market_id,
                lus.user,
                lus.dt,
                lus.collateral,
                lus.collateral_up,
                lus.debt,
                lus.price_oracle,
                lus.soft_liquidation,
                LAG(lus.dt) OVER (PARTITION BY lus.market_id, lus.user ORDER BY lus.dt) as prev_dt
            FROM lending__user_snapshot lus
            WHERE lus.dt >= TIMESTAMP '{extended_start_str}'
            AND lus.dt < TIMESTAMP '{self.end_date}'
            AND lus.debt > 0
        ),
        segmented AS (
            SELECT 
                *,
                CASE 
                    WHEN prev_dt IS NULL THEN 1
                    WHEN EXTRACT(EPOCH FROM (dt - prev_dt)) / 3600 > 5 THEN 1
                    ELSE 0
                END as new_segment
            FROM position_data
        ),
        with_segments AS (
            SELECT 
                *,
                SUM(new_segment) OVER (PARTITION BY market_id, user ORDER BY dt) as segment_id
            FROM segmented
        )
        SELECT 
            ws.market_id,
            ws.user,
            ws.segment_id,
            lm.name as market_name,
            lm.chain_id,
            SPLIT_PART(lm.name, '-', 1) as collateral_token,
            MIN(ws.dt) as first_dt,
            MAX(ws.dt) as last_dt,
            AVG(ws.collateral) as avg_collateral,
            AVG(ws.collateral_up) as avg_collateral_up,
            AVG(ws.debt) as avg_debt,
            AVG(ws.price_oracle) as avg_price_oracle,
            COUNT(*) as records_count,
            SUM(CASE WHEN ws.soft_liquidation = true THEN 1 ELSE 0 END) as soft_liq_count
        FROM with_segments ws
        JOIN lending__markets lm ON lm.id = ws.market_id
        WHERE ws.dt >= TIMESTAMP '{self.start_date}'
        AND ws.dt < TIMESTAMP '{self.end_date}'
        AND ws.soft_liquidation = true
        GROUP BY ws.market_id, ws.user, ws.segment_id, lm.name, lm.chain_id
        ORDER BY avg_debt DESC
        '''
        
        llama_positions = self.execute_sql(query)
        print(f"📈 Найдено {len(llama_positions)} сегментов LlamaLend позиций")
        
        # Аналогично для crvUSD (без сегментации, так как там реже переоткрытия)
        query = f'''
        SELECT 
            cus.controller_id,
            cus.user,
            c.id as controller_id2,
            c.chain_id,
            t.symbol as collateral_token,
            MIN(cus.dt) as first_dt,
            MAX(cus.dt) as last_dt,
            AVG(cus.collateral) as avg_collateral,
            AVG(cus.collateral_up) as avg_collateral_up,
            AVG(cus.debt) as avg_debt,
            AVG(cus.price_oracle) as avg_price_oracle,
            COUNT(*) as records_count,
            SUM(CASE WHEN cus.soft_liquidation = true THEN 1 ELSE 0 END) as soft_liq_count
        FROM crvusd__user_snapshot cus
        JOIN crvusd__controllers c ON c.id = cus.controller_id
        LEFT JOIN tokens t ON t.id = c.collateral_token_id
        WHERE cus.dt >= TIMESTAMP '{self.start_date}'
        AND cus.dt < TIMESTAMP '{self.end_date}'
        AND cus.soft_liquidation = true
        AND cus.debt > 0
        GROUP BY cus.controller_id, cus.user, c.id, c.chain_id, t.symbol
        ORDER BY avg_debt DESC
        '''
        
        crvusd_positions = self.execute_sql(query)
        print(f"📈 Найдено {len(crvusd_positions)} crvUSD позиций")
        
        # Обрабатываем позиции аналогично основному методу
        all_positions = []
        
        # LlamaLend позиции
        for row in llama_positions:
            market_id = row[0]
            user = row[1]
            segment_id = row[2]
            market_name = row[3]
            chain_id = row[4]
            collateral_token = row[5]
            first_dt = row[6]
            last_dt = row[7]
            avg_collateral = float(row[8]) if row[8] else 0
            avg_collateral_up = float(row[9]) if row[9] else 0
            avg_debt = float(row[10]) if row[10] else 0
            avg_price = float(row[11]) if row[11] else 0
            records = row[12]
            soft_liq_count = row[13]
            
            # Получаем precision из маппинга market_id или по символу
            if market_id in self.market_token_map:
                precision = self.market_token_map[market_id]['precision']
                collateral_token = self.market_token_map[market_id]['symbol']
            else:
                precision = self.get_token_precision(collateral_token, chain_id)
            
            # Нормализуем
            collateral_normalized = avg_collateral / precision
            collateral_up_normalized = avg_collateral_up / precision
            
            price_normalized = avg_price / 1e18 if avg_price else 0
            
            if market_id in self.market_borrowed_map:
                borrowed_precision = self.market_borrowed_map[market_id]['precision']
            else:
                borrowed_precision = 1e18
            debt_normalized = avg_debt / borrowed_precision
            
            tvl = collateral_normalized * price_normalized + collateral_up_normalized
            
            position = {
                'platform': 'LlamaLend',
                'chain_name': CHAIN_NAMES.get(chain_id, f'Chain-{chain_id}'),
                'chain_id': chain_id,
                'market_id': market_id,
                'market_name': market_name,
                'user': user,
                'segment_id': segment_id,
                'collateral_token': collateral_token,
                'collateral_amount': collateral_normalized,
                'collateral_up_amount': collateral_up_normalized,
                'debt': debt_normalized,
                'price_oracle': price_normalized,
                'tvl_usd': tvl,
                'start_time': first_dt,
                'end_time': last_dt,
                'duration_hours': 0,
                'data_points': records,
                'soft_liquidation_count': soft_liq_count
            }
            all_positions.append(position)
        
        # crvUSD позиции (аналогично основному методу, без изменений)
        for row in crvusd_positions:
            controller_id = row[0]
            user = row[1]
            chain_id = row[3]
            collateral_token = row[4] or 'Unknown'
            first_dt = row[5]
            last_dt = row[6]
            avg_collateral = float(row[7]) if row[7] else 0
            avg_collateral_up = float(row[8]) if row[8] else 0
            avg_debt = float(row[9]) if row[9] else 0
            avg_price = float(row[10]) if row[10] else 0
            records = row[11]
            soft_liq_count = row[12]
            
            if controller_id in self.controller_token_map:
                precision = self.controller_token_map[controller_id]['precision']
                collateral_token = self.controller_token_map[controller_id]['symbol'] or collateral_token
            else:
                precision = self.get_token_precision(collateral_token, chain_id)
            
            collateral_normalized = avg_collateral / precision
            collateral_up_normalized = avg_collateral_up / precision
            
            price_normalized = avg_price / 1e18 if avg_price else 0
            debt_normalized = avg_debt / 1e18
            
            tvl = collateral_normalized * price_normalized + collateral_up_normalized
            
            position = {
                'platform': 'crvUSD',
                'chain_name': CHAIN_NAMES.get(chain_id, f'Chain-{chain_id}'),
                'chain_id': chain_id,
                'market_id': controller_id,
                'market_name': f'{collateral_token}-crvUSD',
                'user': user,
                'segment_id': 1,  # crvUSD без сегментации
                'collateral_token': collateral_token,
                'collateral_amount': collateral_normalized,
                'collateral_up_amount': collateral_up_normalized,
                'debt': debt_normalized,
                'price_oracle': price_normalized,
                'tvl_usd': tvl,
                'start_time': first_dt,
                'end_time': last_dt,
                'duration_hours': 0,
                'data_points': records,
                'soft_liquidation_count': soft_liq_count
            }
            all_positions.append(position)
        
        print(f"📋 Общее количество позиций/сегментов: {len(all_positions)}")
        return all_positions
    
    def analyze_positions(self):
        """Быстрый анализ позиций без сегментации"""
        print("🔍 Загрузка данных...")
        self.load_chain_names()
        self.load_token_precisions()
        
        print(f"📊 Анализ мягких ликвидаций с {self.start_date} по {self.end_date}")
        
        # Получаем агрегированные данные по позициям за период
        query = f'''
        SELECT 
            lus.market_id,
            lus.user,
            lm.name as market_name,
            lm.chain_id,
            SPLIT_PART(lm.name, '-', 1) as collateral_token,
            MIN(lus.dt) as first_dt,
            MAX(lus.dt) as last_dt,
            AVG(lus.collateral) as avg_collateral,
            AVG(lus.collateral_up) as avg_collateral_up,
            AVG(lus.debt) as avg_debt,
            AVG(lus.price_oracle) as avg_price_oracle,
            COUNT(*) as records_count,
            SUM(CASE WHEN lus.soft_liquidation = true THEN 1 ELSE 0 END) as soft_liq_count
        FROM lending__user_snapshot lus
        JOIN lending__markets lm ON lm.id = lus.market_id
        WHERE lus.dt >= TIMESTAMP '{self.start_date}'
        AND lus.dt < TIMESTAMP '{self.end_date}'
        AND lus.soft_liquidation = true
        AND lus.debt > 0
        GROUP BY lus.market_id, lus.user, lm.name, lm.chain_id
        ORDER BY avg_debt DESC
        '''
        
        llama_positions = self.execute_sql(query)
        print(f"📈 Найдено {len(llama_positions)} LlamaLend позиций")
        
        # Аналогично для crvUSD
        query = f'''
        SELECT 
            cus.controller_id,
            cus.user,
            c.id as controller_id2,
            c.chain_id,
            t.symbol as collateral_token,
            MIN(cus.dt) as first_dt,
            MAX(cus.dt) as last_dt,
            AVG(cus.collateral) as avg_collateral,
            AVG(cus.collateral_up) as avg_collateral_up,
            AVG(cus.debt) as avg_debt,
            AVG(cus.price_oracle) as avg_price_oracle,
            COUNT(*) as records_count,
            SUM(CASE WHEN cus.soft_liquidation = true THEN 1 ELSE 0 END) as soft_liq_count
        FROM crvusd__user_snapshot cus
        JOIN crvusd__controllers c ON c.id = cus.controller_id
        LEFT JOIN tokens t ON t.id = c.collateral_token_id
        WHERE cus.dt >= TIMESTAMP '{self.start_date}'
        AND cus.dt < TIMESTAMP '{self.end_date}'
        AND cus.soft_liquidation = true
        AND cus.debt > 0
        GROUP BY cus.controller_id, cus.user, c.id, c.chain_id, t.symbol
        ORDER BY avg_debt DESC
        '''
        
        crvusd_positions = self.execute_sql(query)
        print(f"📈 Найдено {len(crvusd_positions)} crvUSD позиций")
        
        # Обрабатываем позиции
        all_positions = []
        
        # LlamaLend позиции
        for row in llama_positions:
            market_id = row[0]
            user = row[1]
            market_name = row[2]
            chain_id = row[3]
            collateral_token = row[4]
            first_dt = row[5]
            last_dt = row[6]
            avg_collateral = float(row[7]) if row[7] else 0
            avg_collateral_up = float(row[8]) if row[8] else 0
            avg_debt = float(row[9]) if row[9] else 0
            avg_price = float(row[10]) if row[10] else 0
            records = row[11]
            soft_liq_count = row[12]
            
            # Получаем precision из маппинга market_id или по символу
            if market_id in self.market_token_map:
                precision = self.market_token_map[market_id]['precision']
                # Используем реальный символ токена из БД
                collateral_token = self.market_token_map[market_id]['symbol']
            else:
                precision = self.get_token_precision(collateral_token, chain_id)
            
            # Нормализуем collateral
            collateral_normalized = avg_collateral / precision
            collateral_up_normalized = avg_collateral_up / precision
            
            # Нормализуем price_oracle (в БД хранится умноженным на 1e18)
            price_normalized = avg_price / 1e18 if avg_price else 0
            
            # Нормализуем debt
            if market_id in self.market_borrowed_map:
                borrowed_precision = self.market_borrowed_map[market_id]['precision']
            else:
                borrowed_precision = 1e18  # Стандарт для crvUSD
            debt_normalized = avg_debt / borrowed_precision
            
            # TVL = collateral * price + collateral_up (уже в USD, так как это borrowed токен)
            # collateral_up уже нормализован по borrowed_precision
            tvl = collateral_normalized * price_normalized + collateral_up_normalized
            
            position = {
                'platform': 'LlamaLend',
                'chain_name': CHAIN_NAMES.get(chain_id, f'Chain-{chain_id}'),
                'chain_id': chain_id,
                'market_id': market_id,
                'market_name': market_name,
                'user': user,
                'collateral_token': collateral_token,
                'collateral_amount': collateral_normalized,
                'collateral_up_amount': collateral_up_normalized,
                'debt': debt_normalized,
                'price_oracle': price_normalized,
                'tvl_usd': tvl,
                'start_time': first_dt,
                'end_time': last_dt,
                'duration_hours': 0,  # Не вычисляем для скорости
                'data_points': records,
                'soft_liquidation_count': soft_liq_count
            }
            all_positions.append(position)
        
        # crvUSD позиции
        for row in crvusd_positions:
            controller_id = row[0]
            user = row[1]
            chain_id = row[3]
            collateral_token = row[4] or 'Unknown'
            first_dt = row[5]
            last_dt = row[6]
            avg_collateral = float(row[7]) if row[7] else 0
            avg_collateral_up = float(row[8]) if row[8] else 0
            avg_debt = float(row[9]) if row[9] else 0
            avg_price = float(row[10]) if row[10] else 0
            records = row[11]
            soft_liq_count = row[12]
            
            # Получаем precision из маппинга controller_id или по символу
            if controller_id in self.controller_token_map:
                precision = self.controller_token_map[controller_id]['precision']
                # Используем реальный символ токена из БД
                collateral_token = self.controller_token_map[controller_id]['symbol'] or collateral_token
            else:
                precision = self.get_token_precision(collateral_token, chain_id)
            
            # Нормализуем collateral
            collateral_normalized = avg_collateral / precision
            collateral_up_normalized = avg_collateral_up / precision
            
            # Нормализуем price_oracle (в БД хранится умноженным на 1e18)
            price_normalized = avg_price / 1e18 if avg_price else 0
            
            # Нормализуем debt (crvUSD всегда имеет precision 1e18)
            debt_normalized = avg_debt / 1e18
            
            # TVL = collateral * price + collateral_up (уже в USD, так как это borrowed токен)
            # collateral_up уже нормализован по borrowed_precision
            tvl = collateral_normalized * price_normalized + collateral_up_normalized
            
            position = {
                'platform': 'crvUSD',
                'chain_name': CHAIN_NAMES.get(chain_id, f'Chain-{chain_id}'),
                'chain_id': chain_id,
                'market_id': controller_id,
                'market_name': f'{collateral_token}-crvUSD',
                'user': user,
                'collateral_token': collateral_token,
                'collateral_amount': collateral_normalized,
                'collateral_up_amount': collateral_up_normalized,
                'debt': debt_normalized,
                'price_oracle': price_normalized,
                'tvl_usd': tvl,
                'start_time': first_dt,
                'end_time': last_dt,
                'duration_hours': 0,
                'data_points': records,
                'soft_liquidation_count': soft_liq_count
            }
            all_positions.append(position)
        
        print(f"📋 Общее количество позиций: {len(all_positions)}")
        return all_positions
    
    def generate_report(self, positions):
        """Генерация отчета"""
        if not positions:
            print("❌ Нет данных для анализа")
            return
        
        # Сортируем по TVL
        positions.sort(key=lambda x: x['tvl_usd'], reverse=True)
        
        # Статистика
        total_tvl = sum(p['tvl_usd'] for p in positions)
        total_debt = sum(p['debt'] for p in positions)
        
        # Группируем по токенам
        token_stats = defaultdict(lambda: {'count': 0, 'tvl': 0, 'debt': 0})
        for p in positions:
            token = p['collateral_token']
            token_stats[token]['count'] += 1
            token_stats[token]['tvl'] += p['tvl_usd']
            token_stats[token]['debt'] += p['debt']
        
        # Группируем по сетям
        chain_stats = defaultdict(lambda: {'count': 0, 'tvl': 0, 'debt': 0})
        for p in positions:
            chain = p['chain_name']
            chain_stats[chain]['count'] += 1
            chain_stats[chain]['tvl'] += p['tvl_usd']
            chain_stats[chain]['debt'] += p['debt']
        
        # Группируем по платформам
        platform_stats = defaultdict(lambda: {'count': 0, 'tvl': 0, 'debt': 0})
        for p in positions:
            platform = p['platform']
            platform_stats[platform]['count'] += 1
            platform_stats[platform]['tvl'] += p['tvl_usd']
            platform_stats[platform]['debt'] += p['debt']
        
        # Вывод отчета
        print("\n" + "="*80)
        print("📊 ОТЧЕТ ПО МЯГКИМ ЛИКВИДАЦИЯМ")
        print(f"Период: {self.start_date} - {self.end_date}")
        print("="*80)
        
        print(f"\n💰 ОБЩАЯ СТАТИСТИКА:")
        print(f"  • Всего позиций: {len(positions)}")
        print(f"  • Общий TVL в софт ликвидации: ${total_tvl:,.2f}")
        print(f"  • Общий долг: ${total_debt:,.2f}")
        
        print(f"\n🏦 ПО ПЛАТФОРМАМ:")
        for platform, stats in sorted(platform_stats.items(), key=lambda x: x[1]['tvl'], reverse=True):
            print(f"  • {platform}: {stats['count']} позиций, TVL: ${stats['tvl']:,.2f}, Долг: ${stats['debt']:,.2f}")
        
        print(f"\n🌐 ПО СЕТЯМ:")
        for chain, stats in sorted(chain_stats.items(), key=lambda x: x[1]['tvl'], reverse=True):
            print(f"  • {chain}: {stats['count']} позиций, TVL: ${stats['tvl']:,.2f}, Долг: ${stats['debt']:,.2f}")
        
        print(f"\n🪙 ТОП-10 ТОКЕНОВ ПО TVL:")
        sorted_tokens = sorted(token_stats.items(), key=lambda x: x[1]['tvl'], reverse=True)[:10]
        for token, stats in sorted_tokens:
            print(f"  • {token}: {stats['count']} позиций, TVL: ${stats['tvl']:,.2f}, Долг: ${stats['debt']:,.2f}")
        
        print(f"\n📈 ТОП-20 ПОЗИЦИЙ ПО TVL:")
        for i, p in enumerate(positions[:20], 1):
            print(f"\n{i:2}. {p['market_name']} ({p['platform']}, {p['chain_name']})")
            print(f"    User: {p['user'][:20]}...")
            print(f"    Collateral: {p['collateral_amount']:.6f} {p['collateral_token']}")
            print(f"    Debt: ${p['debt']:,.2f}")
            print(f"    Price: ${p['price_oracle']:.2f}")
            print(f"    TVL: ${p['tvl_usd']:,.2f}")
            print(f"    Data points: {p['data_points']}")
        
        # Сохраняем в JSON
        output_file = f"soft_liquidations_{self.start_date}_{self.end_date}.json"
        with open(output_file, 'w') as f:
            json.dump(positions, f, indent=2, default=str)
        print(f"\n💾 Данные сохранены в {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Анализ софт ликвидаций')
    parser.add_argument('--start', default='2025-02-01', help='Начальная дата (YYYY-MM-DD)')
    parser.add_argument('--end', default='2025-02-05', help='Конечная дата (YYYY-MM-DD)')
    parser.add_argument('--token', help='Metabase session token')
    parser.add_argument('--no-segments', action='store_true', help='Отключить сегментацию позиций (быстрее, но менее точно)')
    
    args = parser.parse_args()
    
    analyzer = SoftLiquidationAnalyzer(args.start, args.end, args.token)
    
    if args.no_segments:
        print("⚡ Быстрый режим без сегментации (может быть менее точным)")
        positions = analyzer.analyze_positions()
    else:
        print("🔄 Анализ с сегментацией позиций (разделение переоткрытий)")
        positions = analyzer.analyze_positions_with_segments()
    
    analyzer.generate_report(positions)

if __name__ == "__main__":
    main()