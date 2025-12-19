import json
import logging
from datetime import date

logger = logging.getLogger(__name__)

def load_path():
    
    file_path = f'./dags/data/YT_data_{date.today()}'
    
    try:
        logger.info(f"Processing file YT_data{date.today()}")

        with open(file_path,'r', encoding='utf-8') as raw_data:
            data = json.load(raw_data)
        
        return data
    except FileNotFoundError as e:
        logger.error(f"File not found: {file_path}")
        raise e
    except json.JSONDecodeError as e:    
        logger.error(f"Invalid JSON in file: {file_path}")
        raise e