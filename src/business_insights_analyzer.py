"""
Модуль для расчета расширенных бизнес-метрик и инсайтов
Фокус на позитивных метриках роста и возможностях бизнеса
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import math

class BusinessInsightsAnalyzer:
    def __init__(self, transaction_data_path, exchange_data_path):
        """
        Инициализация анализатора бизнес-инсайтов
        """
        self.df = pd.read_parquet(transaction_data_path)
        self.exchange_df = pd.read_parquet(exchange_data_path)
        self.df_usd = None
        self._prepare_data()

    def _prepare_data(self):
        """Подготовка данных для анализа"""
        # Конвертация timestamp
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        self.df['date'] = self.df['timestamp'].dt.date
        self.exchange_df['date'] = pd.to_datetime(self.exchange_df['date']).dt.date

        # Добавление временных признаков
        self.df['hour'] = self.df['timestamp'].dt.hour
        self.df['day_of_week'] = self.df['timestamp'].dt.dayofweek
        self.df['week'] = self.df['timestamp'].dt.isocalendar().week
        self.df['is_weekend'] = self.df['day_of_week'].isin([5, 6])

        # Конвертация в USD
        self._convert_to_usd()

    def _convert_to_usd(self):
        """Конвертация всех сумм в USD"""
        merged_df = self.df.merge(self.exchange_df, on='date', how='left')

        def convert_to_usd(row):
            currency = row['currency']
            amount = row['amount']

            if currency == 'USD':
                return amount
            elif currency in ['AUD', 'BRL', 'CAD', 'EUR', 'GBP', 'JPY', 'MXN', 'NGN', 'RUB', 'SGD']:
                exchange_rate = row[currency]
                if pd.notna(exchange_rate) and exchange_rate != 0:
                    return amount / exchange_rate
            return None

        merged_df['amount_usd'] = merged_df.apply(convert_to_usd, axis=1)
        self.df_usd = merged_df.dropna(subset=['amount_usd'])

    def calculate_business_growth_metrics(self):
        """
        Расчет метрик роста и развития бизнеса
        """
        metrics = {}

        # 1. ОБЪЕМЫ И РОСТ БИЗНЕСА
        legit_df = self.df_usd[self.df_usd['is_fraud'] == False]

        # Общие объемы
        total_volume_usd = self.df_usd['amount_usd'].sum()
        legit_volume_usd = legit_df['amount_usd'].sum()

        # Дневные объемы для анализа роста
        daily_volumes = legit_df.groupby('date')['amount_usd'].sum().sort_index()
        daily_transactions = legit_df.groupby('date').size().sort_index()

        # Расчет трендов (первая vs последняя неделя)
        first_week_volume = daily_volumes.head(7).mean()
        last_week_volume = daily_volumes.tail(7).mean()
        volume_growth_rate = ((last_week_volume - first_week_volume) / first_week_volume) * 100

        first_week_transactions = daily_transactions.head(7).mean()
        last_week_transactions = daily_transactions.tail(7).mean()
        transaction_growth_rate = ((last_week_transactions - first_week_transactions) / first_week_transactions) * 100

        metrics['business_growth'] = {
            'total_business_volume_usd': round(total_volume_usd, 2),
            'legitimate_business_volume_usd': round(legit_volume_usd, 2),
            'business_health_percentage': round((legit_volume_usd / total_volume_usd) * 100, 2),
            'daily_average_volume_usd': round(daily_volumes.mean(), 2),
            'daily_average_transactions': round(daily_transactions.mean(), 0),
            'volume_growth_rate_percentage': round(volume_growth_rate, 2),
            'transaction_growth_rate_percentage': round(transaction_growth_rate, 2),
            'peak_daily_volume_usd': round(daily_volumes.max(), 2),
            'consistent_growth_days': len(daily_volumes[daily_volumes > daily_volumes.median()])
        }

        # 2. КЛИЕНТСКАЯ БАЗА И ЛОЯЛЬНОСТЬ
        customer_stats = legit_df.groupby('customer_id').agg({
            'amount_usd': ['sum', 'mean', 'count'],
            'timestamp': ['min', 'max']
        })

        customer_stats.columns = ['total_spent', 'avg_transaction', 'transaction_count', 'first_transaction', 'last_transaction']
        customer_stats['customer_lifetime_days'] = (customer_stats['last_transaction'] - customer_stats['first_transaction']).dt.days
        customer_stats['transactions_per_day'] = customer_stats['transaction_count'] / (customer_stats['customer_lifetime_days'] + 1)

        # Сегментация клиентов по ценности
        customer_stats['value_segment'] = pd.qcut(customer_stats['total_spent'],
                                                 q=5,
                                                 labels=['Bronze', 'Silver', 'Gold', 'Platinum', 'Diamond'])

        # VIP клиенты (топ 10% по объему)
        vip_threshold = customer_stats['total_spent'].quantile(0.9)
        vip_customers = customer_stats[customer_stats['total_spent'] >= vip_threshold]

        metrics['customer_insights'] = {
            'total_active_customers': len(customer_stats),
            'average_customer_lifetime_value_usd': round(customer_stats['total_spent'].mean(), 2),
            'median_customer_lifetime_value_usd': round(customer_stats['total_spent'].median(), 2),
            'vip_customers_count': len(vip_customers),
            'vip_customers_percentage': round((len(vip_customers) / len(customer_stats)) * 100, 2),
            'vip_revenue_contribution_percentage': round((vip_customers['total_spent'].sum() / customer_stats['total_spent'].sum()) * 100, 2),
            'average_transactions_per_customer': round(customer_stats['transaction_count'].mean(), 1),
            'customer_retention_rate': round((len(customer_stats[customer_stats['customer_lifetime_days'] > 7]) / len(customer_stats)) * 100, 2),
            'highly_active_customers': len(customer_stats[customer_stats['transactions_per_day'] > 1]),
            'customer_segments': customer_stats['value_segment'].value_counts().to_dict()
        }

        # 3. ГЕОГРАФИЧЕСКАЯ ЭКСПАНСИЯ И ВОЗМОЖНОСТИ
        country_legit_stats = legit_df.groupby('country').agg({
            'amount_usd': ['sum', 'mean', 'count'],
            'customer_id': 'nunique'
        })

        country_legit_stats.columns = ['total_volume', 'avg_transaction', 'transaction_count', 'unique_customers']
        country_legit_stats['revenue_per_customer'] = country_legit_stats['total_volume'] / country_legit_stats['unique_customers']
        country_legit_stats = country_legit_stats.sort_values('total_volume', ascending=False)

        # Исключаем Unknown City для более точного анализа
        country_legit_clean = legit_df[legit_df['city'] != 'Unknown City']
        city_stats = country_legit_clean.groupby(['country', 'city']).agg({
            'amount_usd': 'sum',
            'customer_id': 'nunique'
        }).reset_index()

        top_markets = country_legit_stats.head(10)
        emerging_markets = country_legit_stats[
            (country_legit_stats['unique_customers'] >= 50) &
            (country_legit_stats['revenue_per_customer'] > country_legit_stats['revenue_per_customer'].median())
        ].head(5)

        metrics['geographic_opportunities'] = {
            'total_countries_served': len(country_legit_stats),
            'total_cities_served': legit_df['city'].nunique(),
            'top_revenue_countries': top_markets.index.tolist()[:5],
            'top_markets_revenue_usd': top_markets['total_volume'].head(5).to_dict(),
            'emerging_high_value_markets': emerging_markets.index.tolist(),
            'average_revenue_per_country_usd': round(country_legit_stats['total_volume'].mean(), 2),
            'market_concentration_top5_percentage': round((top_markets['total_volume'].head(5).sum() / country_legit_stats['total_volume'].sum()) * 100, 2),
            'international_expansion_potential': len(country_legit_stats[country_legit_stats['unique_customers'] < 100]),
            'highest_value_per_customer_country': country_legit_stats['revenue_per_customer'].idxmax(),
            'highest_value_per_customer_amount': round(country_legit_stats['revenue_per_customer'].max(), 2)
        }

        # 4. ПРОДУКТОВАЯ ЛИНЕЙКА И КАТЕГОРИИ
        vendor_legit_stats = legit_df.groupby('vendor_category').agg({
            'amount_usd': ['sum', 'mean', 'count'],
            'customer_id': 'nunique'
        })

        vendor_legit_stats.columns = ['total_revenue', 'avg_transaction', 'transaction_count', 'unique_customers']
        vendor_legit_stats['revenue_per_customer'] = vendor_legit_stats['total_revenue'] / vendor_legit_stats['unique_customers']
        vendor_legit_stats = vendor_legit_stats.sort_values('total_revenue', ascending=False)

        # Анализ каналов
        channel_stats = legit_df.groupby('channel').agg({
            'amount_usd': ['sum', 'mean', 'count']
        })
        channel_stats.columns = ['total_revenue', 'avg_transaction', 'transaction_count']
        channel_stats = channel_stats.sort_values('total_revenue', ascending=False)

        metrics['product_performance'] = {
            'most_profitable_category': vendor_legit_stats.index[0],
            'most_profitable_category_revenue_usd': round(vendor_legit_stats.iloc[0]['total_revenue'], 2),
            'highest_value_category': vendor_legit_stats['avg_transaction'].idxmax(),
            'highest_avg_transaction_usd': round(vendor_legit_stats['avg_transaction'].max(), 2),
            'category_revenue_distribution': vendor_legit_stats['total_revenue'].to_dict(),
            'most_popular_channel': channel_stats.index[0],
            'channel_revenue_distribution': channel_stats['total_revenue'].to_dict(),
            'cross_selling_opportunities': len(vendor_legit_stats[vendor_legit_stats['unique_customers'] < vendor_legit_stats['unique_customers'].median()]),
            'premium_categories_count': len(vendor_legit_stats[vendor_legit_stats['avg_transaction'] > 1000])
        }

        # 5. ОПЕРАЦИОННАЯ ЭФФЕКТИВНОСТЬ
        # Анализ по времени для оптимизации ресурсов
        hourly_legit_stats = legit_df.groupby('hour').agg({
            'amount_usd': ['sum', 'count', 'mean']
        })
        hourly_legit_stats.columns = ['total_revenue', 'transaction_count', 'avg_transaction']

        peak_hours = hourly_legit_stats.nlargest(3, 'total_revenue').index.tolist()
        peak_revenue_hours = hourly_legit_stats.nlargest(3, 'transaction_count').index.tolist()

        # Анализ выходных vs будни
        weekend_stats = legit_df[legit_df['is_weekend']].agg({
            'amount_usd': ['sum', 'mean', 'count']
        })
        weekday_stats = legit_df[~legit_df['is_weekend']].agg({
            'amount_usd': ['sum', 'mean', 'count']
        })

        metrics['operational_efficiency'] = {
            'peak_revenue_hours': peak_hours,
            'peak_transaction_hours': peak_revenue_hours,
            'best_hour_revenue_usd': round(hourly_legit_stats['total_revenue'].max(), 2),
            'weekend_vs_weekday_revenue_ratio': round(weekend_stats['amount_usd']['sum'] / weekday_stats['amount_usd']['sum'], 2),
            'weekend_premium_percentage': round(((weekend_stats['amount_usd']['mean'] - weekday_stats['amount_usd']['mean']) / weekday_stats['amount_usd']['mean']) * 100, 2),
            'processing_volume_per_hour_avg': round(legit_df.groupby('hour').size().mean(), 0),
            'system_utilization_peak_ratio': round(hourly_legit_stats['transaction_count'].max() / hourly_legit_stats['transaction_count'].mean(), 2),
            'revenue_consistency_score': round(100 - (daily_volumes.std() / daily_volumes.mean() * 100), 1)
        }

        # 6. ИННОВАЦИИ И ТЕХНОЛОГИИ
        device_stats = legit_df.groupby('device').agg({
            'amount_usd': ['sum', 'count', 'mean']
        })
        device_stats.columns = ['total_revenue', 'transaction_count', 'avg_transaction']
        device_stats = device_stats.sort_values('total_revenue', ascending=False)

        # Анализ современных vs традиционных каналов
        modern_devices = ['iOS App', 'Android App', 'Chrome', 'Safari', 'Edge']
        modern_transactions = legit_df[legit_df['device'].isin(modern_devices)]
        traditional_transactions = legit_df[~legit_df['device'].isin(modern_devices)]

        card_present_stats = legit_df[legit_df['is_card_present']].agg({'amount_usd': ['sum', 'count', 'mean']})
        card_not_present_stats = legit_df[~legit_df['is_card_present']].agg({'amount_usd': ['sum', 'count', 'mean']})

        metrics['innovation_adoption'] = {
            'top_device_by_revenue': device_stats.index[0],
            'top_device_revenue_usd': round(device_stats.iloc[0]['total_revenue'], 2),
            'mobile_vs_desktop_ratio': round(len(modern_transactions) / len(traditional_transactions), 2) if len(traditional_transactions) > 0 else float('inf'),
            'digital_adoption_percentage': round((len(modern_transactions) / len(legit_df)) * 100, 2),
            'contactless_vs_contact_ratio': round(card_not_present_stats['amount_usd']['count'] / card_present_stats['amount_usd']['count'], 2),
            'contactless_revenue_percentage': round((card_not_present_stats['amount_usd']['sum'] / legit_df['amount_usd'].sum()) * 100, 2),
            'device_diversity_score': len(device_stats),
            'innovation_revenue_premium': round(((modern_transactions['amount_usd'].mean() - traditional_transactions['amount_usd'].mean()) / traditional_transactions['amount_usd'].mean()) * 100, 2) if len(traditional_transactions) > 0 else 0
        }

        return metrics

    def calculate_market_opportunities(self):
        """
        Расчет рыночных возможностей и потенциала роста
        """
        legit_df = self.df_usd[self.df_usd['is_fraud'] == False]
        opportunities = {}

        # 1. НЕИСПОЛЬЗОВАННЫЙ ПОТЕНЦИАЛ КЛИЕНТОВ
        customer_activity = legit_df.groupby('customer_id').agg({
            'amount_usd': ['sum', 'count', 'mean'],
            'timestamp': ['min', 'max']
        })
        customer_activity.columns = ['total_spent', 'transaction_count', 'avg_transaction', 'first_seen', 'last_seen']

        # Клиенты с низкой активностью но высоким потенциалом
        low_activity_high_value = customer_activity[
            (customer_activity['transaction_count'] < customer_activity['transaction_count'].median()) &
            (customer_activity['avg_transaction'] > customer_activity['avg_transaction'].median())
        ]

        # Клиенты, которые давно не совершали покупки
        recent_date = legit_df['timestamp'].max()
        inactive_customers = customer_activity[
            (recent_date - customer_activity['last_seen']).dt.days > 7
        ]

        opportunities['customer_reactivation'] = {
            'low_activity_high_value_customers': len(low_activity_high_value),
            'potential_revenue_from_reactivation_usd': round(low_activity_high_value['avg_transaction'].sum(), 2),
            'inactive_customers_count': len(inactive_customers),
            'inactive_customers_lost_revenue_potential_usd': round(inactive_customers['total_spent'].sum(), 2),
            'average_customer_upside_potential_usd': round(customer_activity['avg_transaction'].quantile(0.75) - customer_activity['avg_transaction'].median(), 2)
        }

        # 2. ГЕОГРАФИЧЕСКАЯ ЭКСПАНСИЯ
        country_performance = legit_df.groupby('country').agg({
            'amount_usd': ['sum', 'mean'],
            'customer_id': 'nunique'
        })
        country_performance.columns = ['total_revenue', 'avg_transaction', 'customer_count']
        country_performance['revenue_per_customer'] = country_performance['total_revenue'] / country_performance['customer_count']

        # Страны с высоким потенциалом (высокий средний чек, но мало клиентов)
        high_potential_countries = country_performance[
            (country_performance['avg_transaction'] > country_performance['avg_transaction'].median()) &
            (country_performance['customer_count'] < country_performance['customer_count'].quantile(0.75))
        ].sort_values('avg_transaction', ascending=False)

        opportunities['geographic_expansion'] = {
            'high_potential_markets': high_potential_countries.index.tolist()[:5],
            'expansion_revenue_potential_usd': round(high_potential_countries['avg_transaction'].sum() * 100, 2),  # Предполагаем привлечение 100 клиентов на рынок
            'underserved_markets_count': len(country_performance[country_performance['customer_count'] < 50]),
            'market_penetration_opportunities': len(high_potential_countries)
        }

        # 3. ПРОДУКТОВЫЕ ВОЗМОЖНОСТИ
        category_cross_sell = legit_df.groupby(['customer_id', 'vendor_category']).size().unstack(fill_value=0)
        customer_category_count = (category_cross_sell > 0).sum(axis=1)

        # Клиенты, использующие только одну категорию
        single_category_customers = customer_category_count[customer_category_count == 1]

        # Потенциал кросс-продаж
        avg_categories_per_customer = customer_category_count.mean()
        max_categories = len(category_cross_sell.columns)

        opportunities['product_expansion'] = {
            'single_category_customers_count': len(single_category_customers),
            'cross_sell_potential_customers': len(customer_category_count[customer_category_count < avg_categories_per_customer]),
            'average_categories_per_customer': round(avg_categories_per_customer, 1),
            'max_category_utilization_potential': max_categories,
            'category_expansion_opportunity_percentage': round(((max_categories - avg_categories_per_customer) / max_categories) * 100, 1)
        }

        return opportunities

    def generate_executive_dashboard_metrics(self):
        """
        Генерация метрик для executive dashboard
        """
        business_metrics = self.calculate_business_growth_metrics()
        opportunities = self.calculate_market_opportunities()

        # Объединение всех метрик
        dashboard = {
            'business_health': business_metrics,
            'market_opportunities': opportunities,
            'generated_at': datetime.now().isoformat(),
            'data_period': {
                'start_date': str(self.df_usd['timestamp'].min().date()),
                'end_date': str(self.df_usd['timestamp'].max().date()),
                'total_days': (self.df_usd['timestamp'].max() - self.df_usd['timestamp'].min()).days
            }
        }

        return dashboard

    def save_business_insights(self, output_dir='results'):
        """
        Сохранение бизнес-инсайтов
        """
        import os
        os.makedirs(output_dir, exist_ok=True)

        dashboard = self.generate_executive_dashboard_metrics()

        # Сохранение в JSON
        with open(f'{output_dir}/business_insights.json', 'w', encoding='utf-8') as f:
            json.dump(dashboard, f, indent=2, ensure_ascii=False)

        # Создание краткого executive summary
        summary = self._generate_business_summary(dashboard)
        with open(f'{output_dir}/business_executive_summary.md', 'w', encoding='utf-8') as f:
            f.write(summary)

        return dashboard

    def _generate_business_summary(self, dashboard):
        """
        Генерация краткого бизнес-отчета
        """
        bg = dashboard['business_health']['business_growth']
        ci = dashboard['business_health']['customer_insights']
        geo = dashboard['business_health']['geographic_opportunities']

        summary = f"""# Executive Business Summary

## 🚀 Ключевые показатели роста

**Общий оборот бизнеса:** ${bg['legitimate_business_volume_usd']:,.2f}
**Здоровье бизнеса:** {bg['business_health_percentage']:.1f}% (доля легитимных операций)
**Рост объемов:** {bg['volume_growth_rate_percentage']:+.1f}% за период
**Рост транзакций:** {bg['transaction_growth_rate_percentage']:+.1f}% за период

## 👥 Клиентская база

**Активных клиентов:** {ci['total_active_customers']:,}
**Средняя ценность клиента:** ${ci['average_customer_lifetime_value_usd']:,.2f}
**VIP клиенты:** {ci['vip_customers_count']:,} ({ci['vip_customers_percentage']:.1f}%)
**Вклад VIP в выручку:** {ci['vip_revenue_contribution_percentage']:.1f}%

## 🌍 География бизнеса

**Стран присутствия:** {geo['total_countries_served']}
**Городов присутствия:** {geo['total_cities_served']}
**Топ-5 рынков:** {', '.join(geo['top_revenue_countries'])}
**Концентрация топ-5:** {geo['market_concentration_top5_percentage']:.1f}% выручки

## 💡 Возможности роста

**Потенциал реактивации клиентов:** ${dashboard['market_opportunities']['customer_reactivation']['potential_revenue_from_reactivation_usd']:,.2f}
**Рынки для экспансии:** {dashboard['market_opportunities']['geographic_expansion']['high_potential_markets'][:3]}
**Клиенты для кросс-продаж:** {dashboard['market_opportunities']['product_expansion']['cross_sell_potential_customers']:,}

---
*Отчет сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
        return summary

if __name__ == "__main__":
    # Пример использования
    analyzer = BusinessInsightsAnalyzer(
        'data/transaction_fraud_data.parquet',
        'data/historical_currency_exchange.parquet'
    )

    dashboard = analyzer.save_business_insights()
    print("Бизнес-инсайты сохранены в папку 'results'")