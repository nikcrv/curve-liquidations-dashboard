#!/usr/bin/env python3
import json
import os
import logging
from web3 import Web3
from web3.middleware import geth_poa_middleware
from datetime import datetime, timezone
import time
import random
import argparse

# Кэш для decimals токенов, чтобы не запрашивать каждый раз
DECIMALS_CACHE = {}
# Кэш для liquidation_discount
DISCOUNT_CACHE = {}

def get_collateral_decimals(w3: Web3, controller_address: str):
    """Получает decimals для collateral токена из контроллера"""
    if controller_address in DECIMALS_CACHE:
        return DECIMALS_CACHE[controller_address]
    
    try:
        # Минимальный ABI для вызова методов контроллера
        controller_abi = [
            {"inputs": [], "name": "collateral_token", "outputs": [{"type": "address"}], "stateMutability": "view", "type": "function"},
        ]
        erc20_abi = [
            {"inputs": [], "name": "decimals", "outputs": [{"type": "uint8"}], "stateMutability": "view", "type": "function"},
        ]
        
        controller = w3.eth.contract(address=controller_address, abi=controller_abi)
        collateral_address = controller.functions.collateral_token().call()
        
        collateral = w3.eth.contract(address=collateral_address, abi=erc20_abi)
        decimals = collateral.functions.decimals().call()
        
        DECIMALS_CACHE[controller_address] = decimals
        logging.info(f"Получены decimals для контроллера {controller_address}: {decimals}")
        return decimals
    except Exception as e:
        logging.warning(f"Не удалось получить decimals для {controller_address}: {e}. Используем 18")
        DECIMALS_CACHE[controller_address] = 18
        return 18

def get_liquidation_discount(w3: Web3, controller_address: str):
    """Получает размер ликвидационного дисконта из контроллера"""
    if controller_address in DISCOUNT_CACHE:
        return DISCOUNT_CACHE[controller_address]
    
    try:
        # Минимальный ABI для вызова liquidation_discount
        controller_abi = [
            {"inputs": [], "name": "liquidation_discount", "outputs": [{"type": "uint256"}], "stateMutability": "view", "type": "function"},
        ]
        
        controller = w3.eth.contract(address=controller_address, abi=controller_abi)
        # liquidation_discount возвращается с 18 decimals (например, 0.06 * 1e18 для 6%)
        discount = controller.functions.liquidation_discount().call()
        discount_percent = float(discount) / 1e18 * 100  # Конвертируем в проценты
        
        DISCOUNT_CACHE[controller_address] = discount_percent
        logging.info(f"Получен liquidation_discount для контроллера {controller_address}: {discount_percent:.2f}%")
        return discount_percent
    except Exception as e:
        # Если не удалось получить, используем типичное значение 6%
        logging.warning(f"Не удалось получить liquidation_discount для {controller_address}: {e}. Используем 6%")
        DISCOUNT_CACHE[controller_address] = 6.0
        return 6.0


# Файл для хранения истории сканирования контрольных точек
HISTORY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "liquidation_history.json")
# Файл для хранения базы ликвидационных событий
LIQUIDATIONS_DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "liquidations_db.json")

def load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading history file: {e}")
            return {}
    return {}

def save_history(history):
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(history, f, indent=2)
    except Exception as e:
        logging.error(f"Error saving history file: {e}")

def load_config():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, "config.json")
    with open(config_path, "r") as f:
        return json.load(f)

def load_liquidations_db():
    if os.path.exists(LIQUIDATIONS_DB_FILE):
        try:
            with open(LIQUIDATIONS_DB_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading liquidations DB: {e}")
            return []
    return []

def save_liquidations_db(db):
    try:
        with open(LIQUIDATIONS_DB_FILE, "w") as f:
            json.dump(db, f, indent=2)
    except Exception as e:
        logging.error(f"Error saving liquidations DB: {e}")

def update_liquidations_db(new_events):
    db = load_liquidations_db()
    existing_tx_hashes = {event["tx_hash"] for event in db}
    for event in new_events:
        if event["tx_hash"] not in existing_tx_hashes:
            db.append(event)
    save_liquidations_db(db)

def scan_liquidations_for_controller(w3: Web3, controller_address: str, from_block: int, net_cfg: dict) -> list:
    """
    Сканирует ликвидационные события для одного контроллера.
    Если погашённый долг (debt_repaid) меньше $5, событие пропускается.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    controller_abi_path = os.path.join(current_dir, "controller_abi.json")
    with open(controller_abi_path, "r") as f:
        controller_abi = json.load(f)
    controller = w3.eth.contract(address=controller_address, abi=controller_abi)
    
    # Рассчитываем хэш сигнатуры события Liquidate
    event_signature = "Liquidate(address,address,uint256,uint256,uint256)"
    event_signature_hash = "0x" + w3.keccak(text=event_signature).hex()
    
    to_block = w3.eth.block_number
    chunk_size = 10000
    events = []
    current_from = from_block
    while current_from <= to_block:
        current_to = min(current_from + chunk_size - 1, to_block)
        logging.info(f"Сканирование контроллера {controller_address}: блоки {current_from} - {current_to}")
        filter_params = {
            "fromBlock": current_from,
            "toBlock": current_to,
            "address": controller_address,
            "topics": [event_signature_hash]
        }
        try:
            logs = w3.eth.get_logs(filter_params)
            logging.info(f"Найдено {len(logs)} событий в блоках {current_from} - {current_to}")
            for log in logs:
                try:
                    event = controller.events.Liquidate().process_log(log)
                    # Вычисляем погашённый долг (debt) в USD (предполагается, что единицы перевода 1e18)
                    if hasattr(event.args, "debt"):
                        debt_repaid = float(event.args.debt) / 1e18
                    else:
                        debt_repaid = None
                    # Если погашённый долг не доступен или меньше $5, пропускаем событие
                    if debt_repaid is None or debt_repaid < 5:
                        logging.info(f"Пропускаем ликвидацию tx {log['transactionHash'].hex()} - погашенный долг {debt_repaid} меньше $5")
                        continue
                    block_obj = w3.eth.get_block(log["blockNumber"])
                    liquidation_time = datetime.fromtimestamp(block_obj.timestamp, tz=timezone.utc)
                    new_event = {
                        "network": None,  # будет заполнено позже на уровне scan_liquidations
                        "controller": controller_address,
                        "block_number": log["blockNumber"],
                        "liquidation_time": liquidation_time.isoformat(),
                        "tx_hash": log["transactionHash"].hex(),
                        "liquidator": event.args.liquidator,
                        "user": event.args.user,
                        "collateral_received": float(event.args.collateral_received) / 1e18 if hasattr(event.args, "collateral_received") else None,
                        "stablecoin_received": float(event.args.stablecoin_received) / 1e18 if hasattr(event.args, "stablecoin_received") else None,
                        "debt_repaid": debt_repaid,
                        "collateral_token": net_cfg.get("collateral_token", "N/A"),
                        "platform": net_cfg.get("platform", "N/A")
                    }
                    events.append(new_event)
                except Exception as e:
                    logging.error(f"Ошибка декодирования лога: {e}")
        except Exception as e:
            logging.error(f"Ошибка при получении логов с блоков {current_from} - {current_to}: {e}")
        current_from = current_to + 1
    return events

def get_block_by_timestamp(w3: Web3, target_timestamp: int, network_name: str, date_str: str) -> int:
    """Находит блок по временной метке через бинарный поиск"""
    # Получаем последний блок с retry логикой
    max_retries = 5
    for retry in range(max_retries):
        try:
            latest_block = w3.eth.block_number
            break
        except Exception as e:
            if "429" in str(e) and retry < max_retries - 1:
                delay = 2 ** retry + random.uniform(0, 2)
                logging.warning(f"Rate limit при получении последнего блока, ждем {delay:.2f} сек")
                time.sleep(delay)
            else:
                logging.error(f"Не удалось получить последний блок после {max_retries} попыток")
                raise
    
    earliest_block = 1
    
    logging.info(f"Поиск блока для даты {date_str} в сети {network_name} (target timestamp: {target_timestamp})")
    
    iteration = 0
    while earliest_block <= latest_block:
        iteration += 1
        mid_block = (earliest_block + latest_block) // 2
        try:
            logging.info(f"Итерация {iteration}: проверяем блок {mid_block} (диапазон: {earliest_block}-{latest_block})")
            
            # Получаем блок с retry логикой для rate limiting
            for retry in range(5):
                try:
                    block = w3.eth.get_block(mid_block)
                    break
                except Exception as e:
                    if "429" in str(e) and retry < 4:
                        delay = 2 ** retry + random.uniform(0, 2)
                        logging.warning(f"Rate limit при получении блока {mid_block}, ждем {delay:.2f} сек")
                        time.sleep(delay)
                    else:
                        raise
            
            block_timestamp = block.timestamp
            
            block_date = datetime.fromtimestamp(block_timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            logging.info(f"Блок {mid_block}: timestamp={block_timestamp}, дата={block_date}")
            
            if abs(block_timestamp - target_timestamp) <= 3600:  # Точность в 1 час
                logging.info(f"Найден подходящий блок {mid_block} для даты {date_str} в сети {network_name}")
                return mid_block
            elif block_timestamp < target_timestamp:
                earliest_block = mid_block + 1
            else:
                latest_block = mid_block - 1
        except Exception as e:
            logging.error(f"Ошибка получения блока {mid_block}: {e}")
            # При POA ошибке пробуем получить текущий блок
            if "POA chain" in str(e) or "extraData" in str(e):
                try:
                    current_block = w3.eth.block_number
                    logging.warning(f"POA ошибка, используем текущий блок {current_block} для {date_str}")
                    return current_block
                except:
                    pass
            break
    
    # Проверка на корректность найденного блока
    if earliest_block > latest_block:
        # Ошибка в бинарном поиске, используем текущий блок
        try:
            current_block = w3.eth.block_number
            logging.warning(f"Ошибка поиска блока для {date_str}, используем текущий блок: {current_block}")
            return current_block
        except:
            logging.error(f"Не удалось получить текущий блок для {network_name}")
            return latest_block
    
    logging.info(f"Завершен поиск блока для {date_str} в {network_name}: найден блок {latest_block}")
    return latest_block

def scan_liquidations(start_date: str = None, end_date: str = None) -> list:
    """
    Сканирует ликвидационные события по всем сетям и контроллерам,
    обновляет базу ликвидаций и возвращает полную базу событий.
    """
    config = load_config()
    history = load_history()
    all_new_events = []
    networks = config.get("networks", {})
    
    # Преобразуем даты в timestamp если заданы
    start_timestamp = None
    end_timestamp = None
    if start_date:
        start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    if end_date:
        end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())

    for network_name, net_cfg in networks.items():
        rpc_url = net_cfg.get("RPC_URL")
        if not rpc_url:
            continue
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        
        # Добавляем POA middleware для Optimism
        if network_name == "optimism":
            w3.middleware_onion.inject(geth_poa_middleware, layer=0)
            logging.info(f"Добавлен POA middleware для сети {network_name}")
        
        if not w3.is_connected():
            logging.error(f"Не удалось подключиться к сети {network_name}")
            continue

        controllers = net_cfg.get("controller_contracts", [])
        # Формируем список контроллеров с их данными и контрольными точками
        controllers_list = []
        for ctrl in controllers:
            controller_address = Web3.to_checksum_address(ctrl["address"])
            creation_block = ctrl.get("creation_block", 0)
            key = f"{network_name}_{controller_address}"
            
            # Определяем стартовый блок
            if start_timestamp:
                start_block = get_block_by_timestamp(w3, start_timestamp, network_name, start_date)
                # Защита от creation_block = 0
                min_creation_block = max(creation_block, 1)  # Минимум блок 1
                effective_start = max(start_block, min_creation_block)
                logging.info(f"Контроллер {controller_address}: будет сканироваться с блока {effective_start}")
            else:
                # Защита от creation_block = 0
                min_creation_block = max(creation_block, 1)  # Минимум блок 1
                effective_start = history.get(key, min_creation_block)
            
            controllers_list.append({
                "address": controller_address,
                "effective_start": effective_start,
                "creation_block": creation_block,
                "collateral_token": ctrl.get("collateral_token", "N/A"),
                "platform": ctrl.get("platform", "N/A"),
                "history_key": key
            })
        if not controllers_list:
            continue

        # Определяем глобальный минимум для текущей сети
        global_min = min(ctrl["effective_start"] for ctrl in controllers_list)
        logging.info(f"Начинается сканирование сети {network_name} с блока {global_min}")

        # Определяем конечный блок
        if end_timestamp:
            to_block = get_block_by_timestamp(w3, end_timestamp, network_name, end_date)
        else:
            to_block = w3.eth.block_number
            
        chunk_size = 10000
        current_from = global_min
        
        logging.info(f"Сканирование сети {network_name}: блоки {global_min} - {to_block}")
        
        # Валидация периода сканирования
        if global_min >= to_block:
            logging.warning(f"Пропускаем сеть {network_name}: начальный блок {global_min} >= конечного блока {to_block}")
            continue

        while current_from <= to_block:
            current_to = min(current_from + chunk_size - 1, to_block)
            # Выбираем адреса контроллеров, для которых current_to >= их effective_start
            addresses = [ctrl["address"] for ctrl in controllers_list if ctrl["effective_start"] <= current_to]
            if not addresses:
                current_from = current_to + 1
                continue

            event_signature = "Liquidate(address,address,uint256,uint256,uint256)"
            event_signature_hash = w3.keccak(text=event_signature).hex()
            filter_params = {
                "fromBlock": current_from,
                "toBlock": current_to,
                "address": addresses,
                "topics": [event_signature_hash]
            }
            retry_count = 0
            max_retries = 5
            while retry_count <= max_retries:
                try:
                    logs = w3.eth.get_logs(filter_params)
                    logging.info(f"Сканирование блоков {current_from} - {current_to} в сети {network_name}: найдено {len(logs)} событий")
                    break  # Успешно получили логи, выходим из retry цикла
                except Exception as e:
                    error_str = str(e)
                    if "429" in error_str or "Too Many Requests" in error_str:
                        retry_count += 1
                        if retry_count <= max_retries:
                            delay = (2 ** retry_count) + random.uniform(0, 2)  # Exponential backoff + jitter
                            logging.warning(f"Ошибка 429 при получении логов блоков {current_from}-{current_to}, попытка {retry_count}/{max_retries}, ждем {delay:.1f}с")
                            time.sleep(delay)
                        else:
                            logging.error(f"Превышено максимальное количество попыток для блоков {current_from} - {current_to}: {e}")
                            break
                    else:
                        logging.error(f"Ошибка при получении логов с блоков {current_from} - {current_to}: {e}")
                        break
            
            # Обрабатываем найденные логи после успешного получения
            if retry_count <= max_retries and 'logs' in locals():
                for log in logs:
                    ctrl_info = next((c for c in controllers_list if c["address"].lower() == log["address"].lower()), None)
                    if ctrl_info is None:
                        continue
                    try:
                        current_dir = os.path.dirname(os.path.abspath(__file__))
                        controller_abi_path = os.path.join(current_dir, "controller_abi.json")
                        with open(controller_abi_path, "r") as f:
                            controller_abi = json.load(f)
                        controller = w3.eth.contract(address=ctrl_info["address"], abi=controller_abi)
                        event = controller.events.Liquidate().process_log(log)
                        
                        # Получаем правильные decimals для collateral токена
                        collateral_decimals = get_collateral_decimals(w3, ctrl_info["address"])
                        
                        # Вычисляем погашённый долг (debt всегда в 18 decimals для crvUSD/stablecoin)
                        if hasattr(event.args, "debt"):
                            debt_repaid = float(event.args.debt) / 1e18
                            logging.info(f"Декодирована ликвидация tx {log['transactionHash'].hex()}: debt_repaid={debt_repaid}")
                        else:
                            debt_repaid = None
                            logging.warning(f"Ликвидация tx {log['transactionHash'].hex()} не имеет поля debt")
                        if debt_repaid is None or debt_repaid < 5:
                            logging.info(f"Пропускаем ликвидацию tx {log['transactionHash'].hex()} - погашенный долг {debt_repaid} меньше $5")
                            continue
                        
                        # Пропускаем самоликвидации (когда пользователь сам закрывает свою позицию)
                        if event.args.liquidator.lower() == event.args.user.lower():
                            logging.info(f"Пропускаем самоликвидацию tx {log['transactionHash'].hex()} - liquidator == user ({event.args.liquidator})")
                            continue
                        
                        # Получаем liquidation_discount
                        liquidation_discount = get_liquidation_discount(w3, ctrl_info["address"])
                        
                        logging.info(f"НАЙДЕНА ВАЛИДНАЯ ЛИКВИДАЦИЯ tx {log['transactionHash'].hex()}: debt=${debt_repaid:.2f}, discount={liquidation_discount:.2f}% в {network_name}")
                        
                        # Рассчитываем количество полученного залога
                        collateral_received = float(event.args.collateral_received) / (10 ** collateral_decimals) if hasattr(event.args, "collateral_received") else None
                        stablecoin_received = float(event.args.stablecoin_received) / 1e18 if hasattr(event.args, "stablecoin_received") else None
                        
                        # Рассчитываем потери пользователя
                        # Ликвидатор получает долг + дисконт, пользователь теряет этот дисконт
                        user_loss_amount = debt_repaid * (liquidation_discount / 100)  # Потери в USD (дисконт)
                        user_loss_percent = liquidation_discount  # Потери в процентах от долга
                        total_loss_value = debt_repaid + user_loss_amount  # Общая сумма потерянных средств (долг + дисконт)
                        
                        logging.info(f"Потери пользователя: долг=${debt_repaid:.2f} + дисконт=${user_loss_amount:.2f} = ${total_loss_value:.2f}")
                        
                        block_obj = w3.eth.get_block(log["blockNumber"])
                        liquidation_time = datetime.fromtimestamp(block_obj.timestamp, tz=timezone.utc)
                        new_event = {
                            "network": network_name,
                            "controller": ctrl_info["address"],
                            "block_number": log["blockNumber"],
                            "liquidation_time": liquidation_time.isoformat(),
                            "tx_hash": log["transactionHash"].hex(),
                            "liquidator": event.args.liquidator,
                            "user": event.args.user,
                            "collateral_received": collateral_received,
                            "stablecoin_received": stablecoin_received,
                            "debt_repaid": debt_repaid,  # Объем ликвидации в USD (сам долг)
                            "liquidation_discount": liquidation_discount,  # Дисконт ликвидатора в %
                            "user_loss_amount": user_loss_amount,  # Потери пользователя в USD (только дисконт)
                            "total_loss_value": total_loss_value,  # Общая сумма потерянных средств (долг + дисконт)
                            "user_loss_percent": user_loss_percent,  # Потери в процентах от долга
                            "collateral_token": ctrl_info["collateral_token"],
                            "platform": ctrl_info["platform"]
                        }
                        all_new_events.append(new_event)
                        # Сохраняем базу после каждого найденного события
                        update_liquidations_db([new_event])
                    except Exception as e:
                        logging.error(f"Ошибка декодирования лога для контроллера {ctrl_info['address']}: {e}")

            # Обновляем контрольную точку для каждого контроллера, если current_to >= его effective_start
            for ctrl in controllers_list:
                if ctrl["effective_start"] <= current_to:
                    history[ctrl["history_key"]] = current_to
                    ctrl["effective_start"] = current_to + 1
            save_history(history)
            current_from = current_to + 1

    # Финальная проверка - все события уже сохранены поштучно
    logging.info(f"Сканирование завершено. Найдено {len(all_new_events)} новых событий")
    return load_liquidations_db()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Сканер ликвидаций Curve/LlamaLend")
    parser.add_argument("--start-date", type=str, help="Дата начала в формате YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, help="Дата окончания в формате YYYY-MM-DD")
    args = parser.parse_args()
    
    # Создаем файл лога с временной меткой
    current_dir = os.path.dirname(os.path.abspath(__file__))
    log_filename = os.path.join(current_dir, f"scanner_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    # Настраиваем логирование в файл и консоль одновременно
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),  # Вывод в консоль
            logging.FileHandler(log_filename, mode='w', encoding='utf-8')  # Запись в файл
        ]
    )
    
    logging.info(f"Логи сохраняются в файл: {log_filename}")
    
    events = scan_liquidations(args.start_date, args.end_date)
    unique_users = set(ev["user"].lower() for ev in events if "user" in ev)
    logging.info(f"Общая база ликвидаций: найдено {len(events)} событий ликвидации по {len(unique_users)} адресам")

