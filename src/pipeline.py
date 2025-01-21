from typing import Dict, Any
import pandas as pd
from datetime import datetime
import os
import logging
import yaml
from src.default_config import DEFAULT_CONFIG


class TitanicDataPipeline:
    def __init__(self, config_path: str = 'config/config_pipeline.yaml'):
        self.base_path = os.path.dirname(os.path.abspath(__file__))
        self.config_path = os.path.join(self.base_path, config_path)
        self.config = self._load_config(config_path)
        self._setup_logging()

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        try:
            print(f"Looking for config at: {config_path}")

            # Create config directory if it doesn't exist
            config_dir = os.path.dirname(config_path)
            if not os.path.exists(config_dir):
                os.makedirs(config_dir)
                print(f"Created config directory at: {config_dir}")

            # If config file doesn't exist, create it with default settings
            if not os.path.exists(config_path):
                print(f"Config file not found. Creating new config file at: {config_path}")
                with open(config_path, 'w') as f:
                    yaml.dump(DEFAULT_CONFIG, f, default_flow_style=False)
                print("Created config file with default settings")
                return DEFAULT_CONFIG

            # If config exists, load it
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)

        except Exception as e:
            logging.warning(f'Error with configuration file: {str(e)}. Using default configuration')
            return DEFAULT_CONFIG

    def _setup_logging(self):
        log_dir = os.path.join(self.base_path, self.config['directories']['log_dir'])
        os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(
            log_dir,
            f'{self.config['logging']['file_prefix']}_{datetime.now().strftime('%Y%m%d')}.log'
        )
        logging.basicConfig(
            level=getattr(logging, self.config['logging']['level'].upper(), logging.INFO),
            format=self.config['logging']['format'],
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

    def create_directory_structure(self):
        for dir_key, dir_path in self.config['directories'].items():
            full_path = os.path.join(self.base_path, dir_path)
            os.makedirs(full_path, exist_ok=True)

    def load_data(self, filename: str) -> pd.DataFrame:
        file_path = os.path.join(self.base_path, self.config['directories']['input_dir'], filename)
        logging.info(f'Logging data from {file_path}')
        return pd.read_csv(file_path)

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info('Starting data cleaning process')
        df_cleaned = df.copy()

        if 'Age' in df_cleaned.columns and self.config['data_cleaning']['fill_missing_values']['age']:
            fill_strategy = self.config['data_cleaning']['fill_missing_values']['age']
            if fill_strategy == 'median':
                df_cleaned['Age'] = df_cleaned['Age'].fillna(df_cleaned['Age'].median())

        if 'Cabin' in df_cleaned.columns and self.config['data_cleaning']['fill_missing_values']['cabin']:
            df_cleaned['Cabin'] = df_cleaned['Cabin'].fillna(
                self.config['data_cleaning']['fill_missing_values']['cabin']
            )

        if 'Embarked' in df_cleaned and self.config['data_cleaning']['fill_missing_values']['embarked']:
            condition = self.config['data_cleaning']['fill_missing_values']['embarked']
            if condition == 'mode':
                df_cleaned['Embarked'] = df_cleaned['Embarked'].fillna(df_cleaned['Embarked'].mode()[0])

        if self.config['data_cleaning']['feature_engineering']['extract_title']:
            df_cleaned['Title'] = df_cleaned['Name'].str.extract(r'([A-Za-z]+)\.', expand=False)

        if self.config['data_cleaning']['feature_engineering']['create_deck']:
            df_cleaned['Deck'] = df_cleaned['Cabin'].str[0]

        if self.config['data_cleaning']['feature_engineering']['family_size']:
            if 'SibSp' in df_cleaned.columns and 'Parch' in df_cleaned.columns:
                df_cleaned['FamilySize'] = df_cleaned['SibSp'] + df_cleaned['Parch'] + 1

        logging.info('Data cleaning completed')
        return df_cleaned

    def save_processed_data(self, df: pd.DataFrame, filename: str):
        """Save the processed dataset."""
        # Create processed data directory
        processed_dir = os.path.join(self.base_path, 'data/processed_data')
        os.makedirs(processed_dir, exist_ok=True)

        # Create filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d')
        output_filename = f"processed_{timestamp}_{filename}"
        output_path = os.path.join(processed_dir, output_filename)

        print(f"Saving processed data to: {output_path}")
        df.to_csv(output_path, index=False)
        logging.info(f"Processed data saved to {output_path}")

    def run_pipeline(self, input_filename: str) -> pd.DataFrame:
        logging.info(f'Starting pipeline execution')
        self.create_directory_structure()
        df = self.load_data(input_filename)
        df_processed = self.clean_data(df)
        self.save_processed_data(df_processed, input_filename)
        logging.info(f'Pipeline execution completed')
        return df_processed


if __name__ == '__main__':
    pipeline = TitanicDataPipeline()
    processed_data = pipeline.run_pipeline('titanic.csv')
    print("\nFirst few rows of processed data:")
    print(processed_data.head())
