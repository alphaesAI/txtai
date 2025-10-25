import logging
import pandas as pd
import json
from pathlib import Path
from typing import Union, Optional

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

class DataTransmitter:
    """
    class to transmit data extracted from database into JSON format.
    can accept either a pandas DataFrame or a CSV file path.
    """

    def __init__(self, input_data: Union[pd.DataFrame, str], save_to_file: bool=False, output_path: Optional[str]=None):
        self.input_data = input_data
        self.save_to_file = save_to_file
        self.output_path = output_path

    def _load_data(self) -> pd.DataFrame:
        """
        Load data from DataFrame or CSV path.
        """
        if isinstance(self.input_data, pd.DataFrame):
            logger.info(f"Using provided DataFrame with shape: {self.input_data}")
            return self.input_data.copy()
        elif isinstance(self.input_data, str):
            csv_path = Path(self.input_data)
            if not csv_path.exists():
                raise FileNotFoundError(f"CSV file not found: {csv_path}")
            logger.info(f"Reading CSV file from: {csv_path}")
            return pd.read_csv(csv_path)
        else:
            raise TypeError("input_data must be a pandas DataFrame or CSV file path")
        
    @staticmethod
    def _to_json_string(df: pd.DataFrame) -> str:
        """
        Convert DataFrame to JSON string (records orientation).
        """
        return df.to_json(orient="records", date_format="iso")
    
    def _save_json_file(self, json_str: str):
        """
        Save JSON string to a file if requested.
        """
        if not self.output_path:
            self.output_path = "data_export.json"
        with open(self.output_path, "w", encoding="utf-8") as f:
            f.write(json_str)
        logger.info(f"JSON data saved to file: {self.output_path}")

    def __call__(self) -> str:
        """
        Orchestrates data transmission: load - convert - optional save.
        """
        try:
            df = self._load_data()
            json_str = self._to_json_string(df)
            logger.info(f"Converted data to JSON string with {len(df)} records")
            if self.save_to_file:
                self._save_json_file(json_str)
            return json_str
        except Exception as e:
            logger.error(f"Failed to transmit data: {e}")
            raise