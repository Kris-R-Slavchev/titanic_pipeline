DEFAULT_CONFIG = {
    'directories': {
        'input_dir': 'data/raw_data',
        'processed_dir': 'data/processed_data',
        'log_dir': 'logs'
    },
    'data_cleaning': {
        'fill_missing_values': {
            'age': 'median',
            'cabin': 'Unknown',
            'embarked': 'mode'
        },
        'feature_engineering': {
            'extract_title': True,
            'create_deck': True,
            'family_size': True
        }
    },
    'file_naming': {
        'processed_prefix': 'processed',
        'output_prefix': 'analysis',
        'date_format': '%Y%m%d'
    },
    'logging': {
        'level': 'INFO',
        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        'file_prefix': 'pipeline'
    }
}
