# sales_price_project
project_name/
│
├── data/
│   ├── raw/
│   ├── interim/
│   ├── processed/
│   └── external/
│
├── notebooks/
│
├── src/
│   ├── data/
│   │   ├── __init__.py
│   │   ├── ingestion.py
│   │   ├── preprocessing.py
│   │   └── validation.py
│   ├── features/
│   │   ├── __init__.py
│   │   ├── engineering.py
│   │   └── selection.py
│   ├── models/
│   │   ├── __init__.py
│   │   ├── training.py
│   │   ├── evaluation.py
│   │   └── inference.py
│   ├── streaming/
│   │   ├── __init__.py
│   │   ├── spark_streaming.py
│   │   ├── kafka_consumer.py
│   │   └── spark_utils.py
│   ├── monitoring/
│   │   ├── __init__.py
│   │   ├── data_monitoring.py
│   │   └── drift_detection.py
│   ├── validation/
│   │   ├── __init__.py
│   │   ├── data_validation.py
│   │   └── evaluator_checkers.py
│   ├── utils/
│   │   ├── __init__.py
│   │   └── helpers.py
│   ├── config/
│   │   ├── __init__.py
│   │   ├── settings.py
│   │   └── logging.yaml
│   ├── aws/
│   │   ├── __init__.py
│   │   ├── sagemaker.py
│   │   ├── lambda.py
│   │   └── s3.py
│   └── lambdas/
│       ├── __init__.py
│       └── lambda_function.py
│
├── tests/
│   ├── data/
│   ├── features/
│   ├── models/
│   ├── streaming/
│   ├── monitoring/
│   └── validation/
│
├── scripts/
│   ├── run_notebook.py
│   └── train_model.py
│
├── config/
│   ├── parameters.yaml
│   └── environments/
│       ├── development.yaml
│       └── production.yaml
│
├── docs/
│
├── requirements.txt
│
├── setup.py
│
├── README.md
│
└── .gitignore
