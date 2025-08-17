"""
Модуль для загрузки и предобработки данных
"""

import pandas as pd
import numpy as np
from typing import Tuple, Optional

class DataLoader:
    """Класс для загрузки и предобработки данных о транзакциях"""

    def __init__(self, transaction_path: str, exchange_path: str):
        """
        Инициализация загрузчика данных

        Args:
            transaction_path: путь к файлу с транзакциями
            exchange_path: путь к файлу с курсами валют
        """
        self.transaction_path = transaction_path
        self.exchange_path = exchange_path
        self.df = None
        self.exchange_df = None

    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Загрузка данных из файлов

        Returns:
            Tuple с DataFrame транзакций и курсов валют
        """
        print("Загрузка данных...")
        self.df = pd.read_parquet(self.transaction_path)
        self.exchange_df = pd.read_parquet(self.exchange_path)

        print(f"Загружено транзакций: {len(self.df):,}")
        print(f"Загружено курсов валют: {len(self.exchange_df):,}")

        return self.df, self.exchange_df

    def preprocess_data(self) -> pd.DataFrame:
        """
        Предобработка данных

        Returns:
            Предобработанный DataFrame
        """
        if self.df is None or self.exchange_df is None:
            self.load_data()

        # Конвертация временных меток
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        self.df['date'] = self.df['timestamp'].dt.date
        self.exchange_df['date'] = pd.to_datetime(self.exchange_df['date']).dt.date

        # Добавление временных признаков
        self.df['hour'] = self.df['timestamp'].dt.hour
        self.df['day_of_week'] = self.df['timestamp'].dt.dayofweek
        self.df['day_name'] = self.df['timestamp'].dt.day_name()
        self.df['is_weekend'] = self.df['day_of_week'].isin([5, 6])
        self.df['is_night'] = (self.df['hour'] >= 22) | (self.df['hour'] <= 6)

        # Извлечение данных из last_hour_activity
        self.df['unique_merchants'] = self.df['last_hour_activity'].apply(
            lambda x: x['unique_merchants'] if x and 'unique_merchants' in x else None
        )
        self.df['num_transactions_last_hour'] = self.df['last_hour_activity'].apply(
            lambda x: x['num_transactions'] if x and 'num_transactions' in x else None
        )
        self.df['total_amount_last_hour'] = self.df['last_hour_activity'].apply(
            lambda x: x['total_amount'] if x and 'total_amount' in x else None
        )

        print("Предобработка данных завершена")
        return self.df

    def convert_to_usd(self) -> pd.DataFrame:
        """
        Конвертация всех сумм в USD

        Returns:
            DataFrame с суммами в USD
        """
        if self.df is None or self.exchange_df is None:
            self.preprocess_data()

        # Соединение с курсами валют
        merged_df = self.df.merge(self.exchange_df, on='date', how='left')

        def convert_amount(row):
            currency = row['currency']
            amount = row['amount']

            if currency == 'USD':
                return amount
            elif currency in ['AUD', 'BRL', 'CAD', 'EUR', 'GBP', 'JPY', 'MXN', 'NGN', 'RUB', 'SGD']:
                exchange_rate = row[currency]
                if pd.notna(exchange_rate) and exchange_rate != 0:
                    return amount / exchange_rate
            return None

        merged_df['amount_usd'] = merged_df.apply(convert_amount, axis=1)
        df_usd = merged_df.dropna(subset=['amount_usd'])

        print(f"Успешно конвертировано транзакций: {len(df_usd):,}")
        return df_usd

    def get_basic_stats(self, df: Optional[pd.DataFrame] = None) -> dict:
        """
        Получение базовой статистики по данным

        Args:
            df: DataFrame для анализа (если None, используется self.df)

        Returns:
            Словарь с базовой статистикой
        """
        if df is None:
            df = self.df

        if df is None:
            raise ValueError("Данные не загружены")

        stats = {
            'total_transactions': len(df),
            'unique_customers': df['customer_id'].nunique(),
            'date_range': {
                'start': df['timestamp'].min(),
                'end': df['timestamp'].max(),
                'days': (df['timestamp'].max() - df['timestamp'].min()).days
            },
            'fraud_stats': {
                'total_fraud': df['is_fraud'].sum(),
                'fraud_rate': df['is_fraud'].mean(),
                'fraud_percentage': df['is_fraud'].mean() * 100
            },
            'currencies': df['currency'].value_counts().to_dict(),
            'countries': df['country'].nunique(),
            'vendor_categories': df['vendor_category'].nunique()
        }

        return stats