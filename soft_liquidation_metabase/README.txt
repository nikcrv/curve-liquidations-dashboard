SOFT LIQUIDATION ANALYZER v11 - BACKUP
=======================================
Дата бэкапа: 2025-09-04
Версия: v11 (with reopenings support)

ОПИСАНИЕ:
Анализатор софт-ликвидаций для Curve Finance / LlamaLend
Поддерживает реоупенинги (повторные открытия позиций)

ОСНОВНЫЕ ФАЙЛЫ:
- soft_liquidation_report_v11.py - главный скрипт
- config.json - конфигурация сетей
- controller_abi.json - ABI контрактов

РЕЗУЛЬТАТЫ СКАНИРОВАНИЯ:
- soft_liquidation_events.json - события
- soft_liquidations_analysis.json - анализ
- all_soft_liquidations_*.json - все ликвидации
- soft_liquidation_users_*.json/csv - данные пользователей
- tvl_soft_liquidation_*.json - TVL анализ

ЗАПУСК:
python3 soft_liquidation_report_v11.py
python3 soft_liquidation_report_v11.py --start-date 2024-07-01 --end-date 2024-09-01

ОСОБЕННОСТИ v11:
✓ Поддержка реоупенингов
✓ Множественные софт-ликвидации одного пользователя
✓ Корректный учет последовательных событий
✓ Детальная статистика по позициям

ОТЛИЧИЯ ОТ ХАРД-ЛИКВИДАЦИЙ:
- Софт = автоматические, частичные, малые потери
- Хард = принудительные, полные, большие потери