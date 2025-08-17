# Быстрый старт

## 🚀 Как запустить анализ

### 1. Установка зависимостей
```bash
pip install -r requirements.txt
```

### 2. Запуск полного анализа
```bash
python run_analysis.py
```

### 3. Интерактивный анализ в Jupyter
```bash
jupyter notebook notebooks/fraud_analysis_eda.ipynb
```

## 📁 Структура проекта

```
itmo_data_analyze/
├── README.md                          # Полное описание проекта
├── CONCLUSIONS.md                     # Выводы и рекомендации
├── GETTING_STARTED.md                 # Инструкции по запуску
├── requirements.txt                   # Зависимости Python
├── run_analysis.py                    # Основной скрипт анализа
├── data/                             # Данные
│   ├── transaction_fraud_data.parquet
│   ├── historical_currency_exchange.parquet
│   └── README.md
├── notebooks/                        # Jupyter notebooks
│   └── fraud_analysis_eda.ipynb     # Разведочный анализ
├── src/                              # Python модули
│   ├── fraud_analyzer.py            # Основной анализатор
│   └── data_loader.py               # Загрузчик данных
├── results/                          # Результаты (создается автоматически)
│   ├── business_metrics.json        # Бизнес-метрики
│   ├── risk_scores.json             # Риск-скоры
│   └── executive_summary.md          # Краткий отчет
└── images/                           # Графики (создается автоматически)
```

## 🎯 Основные результаты

- **Уровень мошенничества:** 19.97%
- **Потенциальная экономия:** $7-12M в год
- **Ключевые риски:** Россия, Мексика, Бразилия, Нигерия
- **Подозрительных клиентов:** 229 (4.7%)

## 📊 Ключевые файлы

1. **README.md** - Полное описание проекта и методологии
2. **CONCLUSIONS.md** - Детальные выводы и бизнес-рекомендации
3. **notebooks/fraud_analysis_eda.ipynb** - Интерактивный анализ
4. **src/fraud_analyzer.py** - Основной код для расчета метрик

## 💡 Следующие шаги

1. Изучите README.md для понимания методологии
2. Запустите run_analysis.py для получения актуальных метрик
3. Откройте Jupyter notebook для интерактивного исследования
4. Ознакомьтесь с CONCLUSIONS.md для бизнес-рекомендаций

---
*Проект готов к презентации и дальнейшему развитию*