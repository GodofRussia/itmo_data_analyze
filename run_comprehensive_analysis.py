#!/usr/bin/env python3
"""
Комплексный скрипт для запуска полного бизнес-анализа
Включает как анализ мошенничества, так и позитивные бизнес-метрики
"""

import sys
import os
sys.path.append('src')

from fraud_analyzer import FraudAnalyzer
from business_insights_analyzer import BusinessInsightsAnalyzer
from data_loader import DataLoader
import json

def main():
    """Основная функция для запуска комплексного анализа"""

    print("🚀 КОМПЛЕКСНЫЙ БИЗНЕС-АНАЛИЗ ФИНАНСОВЫХ ТРАНЗАКЦИЙ")
    print("=" * 70)

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
        print("📊 Инициализация анализаторов...")

        # 1. АНАЛИЗ МОШЕННИЧЕСТВА
        print("\n🔍 ЭТАП 1: Анализ мошенничества и рисков")
        print("-" * 50)
        fraud_analyzer = FraudAnalyzer(transaction_path, exchange_path)
        fraud_metrics, risk_scores = fraud_analyzer.save_results('results')

        # 2. БИЗНЕС-ИНСАЙТЫ И ВОЗМОЖНОСТИ
        print("\n💡 ЭТАП 2: Анализ бизнес-возможностей и роста")
        print("-" * 50)
        business_analyzer = BusinessInsightsAnalyzer(transaction_path, exchange_path)
        business_dashboard = business_analyzer.save_business_insights('results')

        # 3. ОБЪЕДИНЕННЫЙ ОТЧЕТ
        print("\n📈 ЭТАП 3: Генерация объединенного отчета")
        print("-" * 50)

        # Вывод ключевых результатов
        print("\n" + "=" * 70)
        print("📊 ОСНОВНЫЕ РЕЗУЛЬТАТЫ КОМПЛЕКСНОГО АНАЛИЗА")
        print("=" * 70)

        # БИЗНЕС-ЗДОРОВЬЕ
        bg = business_dashboard['business_health']['business_growth']
        ci = business_dashboard['business_health']['customer_insights']
        geo = business_dashboard['business_health']['geographic_opportunities']

        print(f"\n🚀 РОСТ И РАЗВИТИЕ БИЗНЕСА:")
        print(f"💰 Общий оборот бизнеса: ${bg['legitimate_business_volume_usd']:,.2f}")
        print(f"📈 Здоровье бизнеса: {bg['business_health_percentage']:.1f}% (доля легитимных операций)")
        print(f"📊 Рост объемов: {bg['volume_growth_rate_percentage']:+.1f}% за период")
        print(f"🔄 Рост транзакций: {bg['transaction_growth_rate_percentage']:+.1f}% за период")
        print(f"🏆 Пиковый дневной оборот: ${bg['peak_daily_volume_usd']:,.2f}")

        print(f"\n👥 КЛИЕНТСКАЯ БАЗА:")
        print(f"🎯 Активных клиентов: {ci['total_active_customers']:,}")
        print(f"💎 Средняя ценность клиента: ${ci['average_customer_lifetime_value_usd']:,.2f}")
        print(f"⭐ VIP клиенты: {ci['vip_customers_count']:,} ({ci['vip_customers_percentage']:.1f}%)")
        print(f"💰 Вклад VIP в выручку: {ci['vip_revenue_contribution_percentage']:.1f}%")
        print(f"🔄 Удержание клиентов: {ci['customer_retention_rate']:.1f}%")

        print(f"\n🌍 ГЕОГРАФИЯ И РЫНКИ:")
        print(f"🗺️  Стран присутствия: {geo['total_countries_served']}")
        print(f"🏙️  Городов присутствия: {geo['total_cities_served']}")
        print(f"🏆 Топ-5 рынков: {', '.join(geo['top_revenue_countries'])}")
        print(f"📊 Концентрация топ-5: {geo['market_concentration_top5_percentage']:.1f}% выручки")
        print(f"🚀 Возможности экспансии: {geo['international_expansion_potential']} стран")

        # ПРОДУКТОВАЯ ЛИНЕЙКА
        pp = business_dashboard['business_health']['product_performance']
        print(f"\n🛍️  ПРОДУКТОВАЯ ЛИНЕЙКА:")
        print(f"🥇 Самая прибыльная категория: {pp['most_profitable_category']}")
        print(f"💰 Выручка топ-категории: ${pp['most_profitable_category_revenue_usd']:,.2f}")
        print(f"💎 Категория с высоким чеком: {pp['highest_value_category']} (${pp['highest_avg_transaction_usd']:,.2f})")
        print(f"🏆 Лучший канал: {pp['most_popular_channel']}")

        # ОПЕРАЦИОННАЯ ЭФФЕКТИВНОСТЬ
        oe = business_dashboard['business_health']['operational_efficiency']
        print(f"\n⚡ ОПЕРАЦИОННАЯ ЭФФЕКТИВНОСТЬ:")
        print(f"🕐 Пиковые часы выручки: {', '.join(map(str, oe['peak_revenue_hours']))}")
        print(f"💰 Лучший час: ${oe['best_hour_revenue_usd']:,.2f}")
        print(f"📊 Премия выходных: {oe['weekend_premium_percentage']:+.1f}%")
        print(f"🎯 Стабильность выручки: {oe['revenue_consistency_score']:.1f}/100")

        # ИННОВАЦИИ
        ia = business_dashboard['business_health']['innovation_adoption']
        print(f"\n🚀 ЦИФРОВЫЕ ИННОВАЦИИ:")
        print(f"📱 Топ-устройство: {ia['top_device_by_revenue']} (${ia['top_device_revenue_usd']:,.2f})")
        print(f"📊 Цифровое проникновение: {ia['digital_adoption_percentage']:.1f}%")
        print(f"💳 Бесконтактные платежи: {ia['contactless_revenue_percentage']:.1f}% выручки")
        print(f"🔄 Мобильные vs десктоп: {ia['mobile_vs_desktop_ratio']:.1f}x")

        # ВОЗМОЖНОСТИ РОСТА
        opportunities = business_dashboard['market_opportunities']
        cr = opportunities['customer_reactivation']
        ge = opportunities['geographic_expansion']
        pe = opportunities['product_expansion']

        print(f"\n💡 ВОЗМОЖНОСТИ РОСТА:")
        print(f"🔄 Потенциал реактивации: ${cr['potential_revenue_from_reactivation_usd']:,.2f}")
        print(f"👥 Неактивных клиентов: {cr['inactive_customers_count']:,}")
        print(f"🌍 Рынки для экспансии: {', '.join(ge['high_potential_markets'][:3])}")
        print(f"🛍️  Кросс-продажи: {pe['cross_sell_potential_customers']:,} клиентов")
        print(f"📈 Потенциал категорий: {pe['category_expansion_opportunity_percentage']:.1f}%")

        # РИСКИ И БЕЗОПАСНОСТЬ (из анализа мошенничества)
        financial = fraud_metrics['financial']
        print(f"\n⚠️  РИСКИ И БЕЗОПАСНОСТЬ:")
        print(f"🚨 Потери от мошенничества: ${financial['total_fraud_loss_usd']:,.2f}")
        print(f"📊 Доля потерь: {financial['fraud_loss_percentage']:.2f}% от оборота")
        print(f"💰 Потенциальная экономия (50%): ${fraud_metrics['potential_impact']['potential_savings_50_percent_usd']:,.2f}")
        print(f"🎯 Потенциальная экономия (80%): ${fraud_metrics['potential_impact']['potential_savings_80_percent_usd']:,.2f}")

        # ИТОГОВЫЕ РЕКОМЕНДАЦИИ
        print(f"\n" + "=" * 70)
        print("🎯 КЛЮЧЕВЫЕ РЕКОМЕНДАЦИИ")
        print("=" * 70)

        print("1. 🚀 РОСТ БИЗНЕСА:")
        print(f"   • Фокус на VIP клиентов ({ci['vip_customers_percentage']:.1f}% дают {ci['vip_revenue_contribution_percentage']:.1f}% выручки)")
        print(f"   • Реактивация {cr['inactive_customers_count']:,} неактивных клиентов")
        print(f"   • Экспансия в {len(ge['high_potential_markets'])} перспективных рынков")

        print("\n2. 💡 ПРОДУКТОВЫЕ ВОЗМОЖНОСТИ:")
        print(f"   • Кросс-продажи для {pe['cross_sell_potential_customers']:,} клиентов")
        print(f"   • Развитие категории {pp['highest_value_category']} (высокий чек)")
        print(f"   • Усиление канала {pp['most_popular_channel']}")

        print("\n3. 🔒 БЕЗОПАСНОСТЬ:")
        print(f"   • Снижение мошенничества может дать ${fraud_metrics['potential_impact']['potential_savings_80_percent_usd']:,.2f} экономии")
        print(f"   • Фокус на географические риски (топ-4 страны)")
        print(f"   • Поведенческий мониторинг клиентов")

        print("\n4. ⚡ ОПЕРАЦИОННАЯ ЭФФЕКТИВНОСТЬ:")
        print(f"   • Оптимизация ресурсов в пиковые часы: {', '.join(map(str, oe['peak_revenue_hours']))}")
        print(f"   • Использование премии выходных ({oe['weekend_premium_percentage']:+.1f}%)")
        print(f"   • Развитие цифровых каналов ({ia['digital_adoption_percentage']:.1f}% проникновение)")

        # ФИНАЛЬНАЯ СВОДКА
        total_business_value = bg['legitimate_business_volume_usd']
        total_growth_potential = cr['potential_revenue_from_reactivation_usd'] + ge['expansion_revenue_potential_usd']
        fraud_savings_potential = fraud_metrics['potential_impact']['potential_savings_80_percent_usd']

        print(f"\n" + "=" * 70)
        print("💰 ФИНАЛЬНАЯ ЭКОНОМИЧЕСКАЯ ОЦЕНКА")
        print("=" * 70)
        print(f"📊 Текущий здоровый оборот: ${total_business_value:,.2f}")
        print(f"🚀 Потенциал роста: ${total_growth_potential:,.2f}")
        print(f"🔒 Экономия от безопасности: ${fraud_savings_potential:,.2f}")
        print(f"💎 ОБЩИЙ ПОТЕНЦИАЛ: ${total_growth_potential + fraud_savings_potential:,.2f}")
        print(f"📈 Потенциальный рост бизнеса: {((total_growth_potential + fraud_savings_potential) / total_business_value * 100):.1f}%")

        print("\n" + "=" * 70)
        print("✅ КОМПЛЕКСНЫЙ АНАЛИЗ ЗАВЕРШЕН УСПЕШНО!")
        print("📁 Все результаты сохранены в папку 'results/'")
        print("📊 Файлы результатов:")
        print("   • business_insights.json - Детальные бизнес-метрики")
        print("   • business_executive_summary.md - Краткий бизнес-отчет")
        print("   • business_metrics.json - Метрики мошенничества")
        print("   • executive_summary.md - Отчет по безопасности")
        print("   • risk_scores.json - Риск-скоры")
        print("=" * 70)

    except Exception as e:
        print(f"❌ Ошибка при выполнении анализа: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()