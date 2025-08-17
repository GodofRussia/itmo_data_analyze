"""
Модуль для анализа мошенничества и расчета ключевых бизнес-метрик
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime
import math

class FraudAnalyzer:
    def __init__(self, transaction_data_path, exchange_data_path):
        """
        Инициализация анализатора мошенничества

        Args:
            transaction_data_path: путь к файлу с транзакциями
            exchange_data_path: путь к файлу с курсами валют
        """
        self.df = pd.read_parquet(transaction_data_path)
        self.exchange_df = pd.read_parquet(exchange_data_path)
        self.df_usd = None  # Данные с суммами в USD
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
        self.df['is_night'] = (self.df['hour'] >= 22) | (self.df['hour'] <= 6)

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

    def calculate_key_business_metrics(self):
        """
        Расчет ключевых бизнес-метрик для антифрод системы
        """
        metrics = {}

        # 1. ФИНАНСОВЫЕ МЕТРИКИ
        fraud_df = self.df_usd[self.df_usd['is_fraud'] == True]
        legit_df = self.df_usd[self.df_usd['is_fraud'] == False]

        # Общие финансовые потери от мошенничества
        total_fraud_loss = fraud_df['amount_usd'].sum()
        total_transaction_volume = self.df_usd['amount_usd'].sum()

        metrics['financial'] = {
            'total_fraud_loss_usd': round(total_fraud_loss, 2),
            'total_transaction_volume_usd': round(total_transaction_volume, 2),
            'fraud_loss_percentage': round((total_fraud_loss / total_transaction_volume) * 100, 2),
            'average_fraud_amount_usd': round(fraud_df['amount_usd'].mean(), 2),
            'average_legit_amount_usd': round(legit_df['amount_usd'].mean(), 2),
            'fraud_amount_multiplier': round(fraud_df['amount_usd'].mean() / legit_df['amount_usd'].mean(), 2)
        }

        # 2. ОПЕРАЦИОННЫЕ МЕТРИКИ
        total_transactions = len(self.df_usd)
        fraud_transactions = len(fraud_df)

        metrics['operational'] = {
            'total_transactions': total_transactions,
            'fraud_transactions': fraud_transactions,
            'fraud_rate_percentage': round((fraud_transactions / total_transactions) * 100, 2),
            'transactions_per_day': round(total_transactions / 31, 0),  # 31 день в датасете
            'fraud_transactions_per_day': round(fraud_transactions / 31, 0),
            'unique_customers': self.df_usd['customer_id'].nunique(),
            'avg_transactions_per_customer': round(total_transactions / self.df_usd['customer_id'].nunique(), 1)
        }

        # 3. ГЕОГРАФИЧЕСКИЕ РИСКИ
        country_fraud_stats = self.df_usd.groupby('country').agg({
            'is_fraud': ['count', 'sum', 'mean'],
            'amount_usd': 'sum'
        }).round(3)

        country_fraud_stats.columns = ['total_transactions', 'fraud_transactions', 'fraud_rate', 'total_volume_usd']
        country_fraud_stats = country_fraud_stats.sort_values('fraud_transactions', ascending=False)

        # Топ-5 самых рискованных стран
        top_risk_countries = country_fraud_stats.head(5).to_dict('index')

        metrics['geographic_risk'] = {
            'top_risk_countries': top_risk_countries,
            'countries_with_fraud': len(country_fraud_stats[country_fraud_stats['fraud_transactions'] > 0]),
            'high_risk_countries_count': len(country_fraud_stats[country_fraud_stats['fraud_rate'] > 0.3])
        }

        # 4. ВРЕМЕННЫЕ ПАТТЕРНЫ
        hourly_fraud = self.df_usd.groupby('hour')['is_fraud'].agg(['count', 'sum', 'mean'])
        peak_fraud_hour = hourly_fraud['mean'].idxmax()

        night_fraud_rate = self.df_usd[self.df_usd['is_night']]['is_fraud'].mean()
        day_fraud_rate = self.df_usd[~self.df_usd['is_night']]['is_fraud'].mean()

        metrics['temporal_patterns'] = {
            'peak_fraud_hour': int(peak_fraud_hour),
            'peak_fraud_rate': round(hourly_fraud.loc[peak_fraud_hour, 'mean'], 3),
            'night_fraud_rate': round(night_fraud_rate, 3),
            'day_fraud_rate': round(day_fraud_rate, 3),
            'night_vs_day_risk_ratio': round(night_fraud_rate / day_fraud_rate, 2)
        }

        # 5. ПОВЕДЕНЧЕСКИЕ МЕТРИКИ
        # Анализ активности клиентов
        def extract_unique_merchants(activity):
            if activity and 'unique_merchants' in activity:
                return activity['unique_merchants']
            return None

        self.df_usd['unique_merchants'] = self.df_usd['last_hour_activity'].apply(extract_unique_merchants)
        customer_medians = self.df_usd.groupby('customer_id')['unique_merchants'].median()

        quantile_95 = customer_medians.quantile(0.95)
        high_activity_customers = (customer_medians > quantile_95).sum()

        # Анализ мошенничества среди высокоактивных клиентов
        high_activity_customer_ids = customer_medians[customer_medians > quantile_95].index
        high_activity_fraud_rate = self.df_usd[
            self.df_usd['customer_id'].isin(high_activity_customer_ids)
        ]['is_fraud'].mean()

        metrics['behavioral'] = {
            'high_activity_customers_count': int(high_activity_customers),
            'high_activity_customers_percentage': round((high_activity_customers / len(customer_medians)) * 100, 2),
            'high_activity_fraud_rate': round(high_activity_fraud_rate, 3),
            'median_unique_merchants_95th_percentile': round(quantile_95, 1),
            'avg_customer_unique_merchants': round(customer_medians.mean(), 1)
        }

        # 6. КАТЕГОРИИ ПРОДАВЦОВ
        vendor_analysis = self.df_usd.groupby('vendor_category').agg({
            'is_fraud': ['count', 'sum', 'mean'],
            'amount_usd': ['sum', 'mean']
        }).round(3)

        vendor_analysis.columns = ['total_transactions', 'fraud_transactions', 'fraud_rate', 'total_volume_usd', 'avg_amount_usd']
        vendor_analysis = vendor_analysis.sort_values('fraud_rate', ascending=False)

        # Высокорисковые продавцы
        high_risk_vendor_fraud_rate = self.df_usd[self.df_usd['is_high_risk_vendor']]['is_fraud'].mean()
        low_risk_vendor_fraud_rate = self.df_usd[~self.df_usd['is_high_risk_vendor']]['is_fraud'].mean()

        metrics['vendor_risk'] = {
            'high_risk_vendor_fraud_rate': round(high_risk_vendor_fraud_rate, 3),
            'low_risk_vendor_fraud_rate': round(low_risk_vendor_fraud_rate, 3),
            'risk_vendor_multiplier': round(high_risk_vendor_fraud_rate / low_risk_vendor_fraud_rate, 2),
            'riskiest_vendor_category': vendor_analysis.index[0],
            'riskiest_category_fraud_rate': round(vendor_analysis.iloc[0]['fraud_rate'], 3)
        }

        # 7. КАНАЛЫ И УСТРОЙСТВА
        device_fraud = self.df_usd.groupby('device')['is_fraud'].agg(['count', 'mean']).sort_values('mean', ascending=False)
        channel_fraud = self.df_usd.groupby('channel')['is_fraud'].agg(['count', 'mean']).sort_values('mean', ascending=False)

        metrics['device_channel'] = {
            'riskiest_device': device_fraud.index[0],
            'riskiest_device_fraud_rate': round(device_fraud.iloc[0]['mean'], 3),
            'riskiest_channel': channel_fraud.index[0],
            'riskiest_channel_fraud_rate': round(channel_fraud.iloc[0]['mean'], 3),
            'card_present_fraud_rate': round(self.df_usd[self.df_usd['is_card_present']]['is_fraud'].mean(), 3),
            'card_not_present_fraud_rate': round(self.df_usd[~self.df_usd['is_card_present']]['is_fraud'].mean(), 3)
        }

        # 8. ПОТЕНЦИАЛЬНАЯ ЭКОНОМИЯ
        # Если бы мы могли предотвратить 50% мошенничества
        potential_savings_50 = total_fraud_loss * 0.5
        potential_savings_80 = total_fraud_loss * 0.8

        metrics['potential_impact'] = {
            'potential_savings_50_percent_usd': round(potential_savings_50, 2),
            'potential_savings_80_percent_usd': round(potential_savings_80, 2),
            'monthly_fraud_loss_usd': round(total_fraud_loss / 31 * 30, 2),  # Месячные потери
            'annual_fraud_loss_projection_usd': round(total_fraud_loss / 31 * 365, 2)  # Годовые потери
        }

        return metrics

    def generate_risk_scores(self):
        """
        Генерация риск-скоров для различных сегментов
        """
        risk_scores = {}

        # Риск-скоры по странам (нормализованные от 0 до 100)
        country_fraud_rates = self.df_usd.groupby('country')['is_fraud'].mean()
        max_fraud_rate = country_fraud_rates.max()
        country_risk_scores = (country_fraud_rates / max_fraud_rate * 100).round(1)

        risk_scores['country_risk_scores'] = country_risk_scores.to_dict()

        # Риск-скоры по времени
        hourly_fraud_rates = self.df_usd.groupby('hour')['is_fraud'].mean()
        max_hourly_fraud = hourly_fraud_rates.max()
        hourly_risk_scores = (hourly_fraud_rates / max_hourly_fraud * 100).round(1)

        risk_scores['hourly_risk_scores'] = hourly_risk_scores.to_dict()

        # Риск-скоры по категориям продавцов
        vendor_fraud_rates = self.df_usd.groupby('vendor_category')['is_fraud'].mean()
        max_vendor_fraud = vendor_fraud_rates.max()
        vendor_risk_scores = (vendor_fraud_rates / max_vendor_fraud * 100).round(1)

        risk_scores['vendor_category_risk_scores'] = vendor_risk_scores.to_dict()

        return risk_scores

    def save_results(self, output_dir='results'):
        """
        Сохранение результатов анализа
        """
        import os
        os.makedirs(output_dir, exist_ok=True)

        # Расчет метрик
        metrics = self.calculate_key_business_metrics()
        risk_scores = self.generate_risk_scores()

        # Сохранение в JSON
        with open(f'{output_dir}/business_metrics.json', 'w', encoding='utf-8') as f:
            json.dump(metrics, f, indent=2, ensure_ascii=False)

        with open(f'{output_dir}/risk_scores.json', 'w', encoding='utf-8') as f:
            json.dump(risk_scores, f, indent=2, ensure_ascii=False)

        # Создание краткого отчета
        report = self._generate_executive_summary(metrics)
        with open(f'{output_dir}/executive_summary.md', 'w', encoding='utf-8') as f:
            f.write(report)

        return metrics, risk_scores

    def _generate_executive_summary(self, metrics):
        """
        Генерация краткого отчета для руководства
        """
        report = f"""# Executive Summary: Fraud Analysis Report

## 🚨 Критические показатели

**Общие потери от мошенничества:** ${metrics['financial']['total_fraud_loss_usd']:,.2f}
**Доля мошеннических операций:** {metrics['operational']['fraud_rate_percentage']}%
**Средний ущерб от одной мошеннической операции:** ${metrics['financial']['average_fraud_amount_usd']:,.2f}

## 📊 Ключевые метрики

### Финансовое влияние
- Мошеннические операции составляют {metrics['financial']['fraud_loss_percentage']}% от общего оборота
- Средняя мошенническая операция в {metrics['financial']['fraud_amount_multiplier']}x раз больше легитимной
- Прогнозируемые годовые потери: ${metrics['potential_impact']['annual_fraud_loss_projection_usd']:,.2f}

### Операционные показатели
- Обрабатывается {metrics['operational']['transactions_per_day']:,.0f} транзакций в день
- Из них {metrics['operational']['fraud_transactions_per_day']:,.0f} мошеннических
- {metrics['operational']['unique_customers']:,} активных клиентов

### Географические риски
- {metrics['geographic_risk']['countries_with_fraud']} стран с зафиксированным мошенничеством
- {metrics['geographic_risk']['high_risk_countries_count']} стран с критически высоким уровнем риска (>30%)

### Поведенческие аномалии
- {metrics['behavioral']['high_activity_customers_count']} клиентов ({metrics['behavioral']['high_activity_customers_percentage']}%) демонстрируют подозрительную активность
- Уровень мошенничества среди высокоактивных клиентов: {metrics['behavioral']['high_activity_fraud_rate']:.1%}

## 💰 Потенциальная экономия

При улучшении системы детекции на 50%: **${metrics['potential_impact']['potential_savings_50_percent_usd']:,.2f}**
При улучшении системы детекции на 80%: **${metrics['potential_impact']['potential_savings_80_percent_usd']:,.2f}**

## 🎯 Приоритетные направления

1. **Географическая сегментация** - фокус на топ-5 рискованных стран
2. **Поведенческий анализ** - мониторинг высокоактивных клиентов
3. **Временные паттерны** - усиленный контроль в пиковые часы мошенничества
4. **Категории продавцов** - дополнительные проверки для рискованных категорий

---
*Отчет сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
        return report

if __name__ == "__main__":
    # Пример использования
    analyzer = FraudAnalyzer(
        'data/transaction_fraud_data.parquet',
        'data/historical_currency_exchange.parquet'
    )

    metrics, risk_scores = analyzer.save_results()
    print("Анализ завершен. Результаты сохранены в папку 'results'")