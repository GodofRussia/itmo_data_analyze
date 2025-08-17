#!/usr/bin/env python3
"""
Основной скрипт для запуска анализа мошенничества
"""

import sys
import os
sys.path.append('src')

from fraud_analyzer import FraudAnalyzer
from data_loader import DataLoader
import json

def main():
    """Основная функция для запуска анализа"""

    print("🔍 АНАЛИЗ МОШЕННИЧЕСТВА В ФИНАНСОВЫХ ТРАНЗАКЦИЯХ")
    print("=" * 60)

    # Пути к данным
    transaction_path = 'data/transaction_fraud_data.parquet'
    exchange_path = 'data/historical_currency_exchange.parquet'

    # Проверка существования файлов
    if not os.path.exists(transaction_path):
        print(f"❌ Файл {transaction_path} не найден!")
        return

    if not os.path.exists(exchange_path):
        print(f"❌ Файл {exchange_path} не найден!")
        return

    try:
        # Инициализация анализатора
        print("📊 Инициализация анализатора...")
        analyzer = FraudAnalyzer(transaction_path, exchange_path)

        # Расчет метрик
        print("🧮 Расчет ключевых бизнес-метрик...")
        metrics, risk_scores = analyzer.save_results('results')

        # Вывод основных результатов
        print("\n" + "=" * 60)
        print("📈 ОСНОВНЫЕ РЕЗУЛЬТАТЫ АНАЛИЗА")
        print("=" * 60)

        # Финансовые метрики
        financial = metrics['financial']
        print(f"💰 Общие потери от мошенничества: ${financial['total_fraud_loss_usd']:,.2f}")
        print(f"📊 Доля потерь от оборота: {financial['fraud_loss_percentage']:.2f}%")
        print(f"💳 Средняя мошенническая операция: ${financial['average_fraud_amount_usd']:,.2f}")
        print(f"🔢 Мошенничество больше легитимных в {financial['fraud_amount_multiplier']:.1f}x раз")

        # Операционные метрики
        operational = metrics['operational']
        print(f"\n🏢 Общее количество транзакций: {operational['total_transactions']:,}")
        print(f"⚠️  Мошеннических транзакций: {operational['fraud_transactions']:,}")
        print(f"📈 Уровень мошенничества: {operational['fraud_rate_percentage']:.1f}%")
        print(f"👥 Уникальных клиентов: {operational['unique_customers']:,}")

        # Географические риски
        geographic = metrics['geographic_risk']
        print(f"\n🌍 Стран с мошенничеством: {geographic['countries_with_fraud']}")
        print(f"🚨 Высокорисковых стран (>30%): {geographic['high_risk_countries_count']}")

        # Поведенческие метрики
        behavioral = metrics['behavioral']
        print(f"\n👤 Подозрительно активных клиентов: {behavioral['high_activity_customers_count']} ({behavioral['high_activity_customers_percentage']:.1f}%)")
        print(f"🎯 Уровень мошенничества среди них: {behavioral['high_activity_fraud_rate']:.1%}")

        # Потенциальная экономия
        potential = metrics['potential_impact']
        print(f"\n💡 ПОТЕНЦИАЛЬНАЯ ЭКОНОМИЯ:")
        print(f"   При улучшении на 50%: ${potential['potential_savings_50_percent_usd']:,.2f}")
        print(f"   При улучшении на 80%: ${potential['potential_savings_80_percent_usd']:,.2f}")
        print(f"   Прогноз годовых потерь: ${potential['annual_fraud_loss_projection_usd']:,.2f}")

        print("\n" + "=" * 60)
        print("✅ АНАЛИЗ ЗАВЕРШЕН УСПЕШНО!")
        print("📁 Результаты сохранены в папку 'results/'")
        print("📊 Подробный отчет: results/executive_summary.md")
        print("📈 Метрики: results/business_metrics.json")
        print("🎯 Риск-скоры: results/risk_scores.json")
        print("=" * 60)

    except Exception as e:
        print(f"❌ Ошибка при выполнении анализа: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()