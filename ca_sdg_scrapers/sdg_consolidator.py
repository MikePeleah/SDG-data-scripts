from config import Config
from pathlib import Path
from typing import Union
import pandas as pd
from tqdm import tqdm

class Consolidator(object):
    def __init__(self, path_to_config: Union[str, Path, None] = None) -> None:
        self.config = Config(path_to_config)
    
    def consolidate_data(self, country_ids: list[str]):
        """Consolidate data from the source and save it as CSV files."""
        country_configs = [Config(self.config.path_to_config, country_id=country_id) for country_id in country_ids]
        processed_file_lists = [country_config.get_processed_file_list() for country_config in country_configs]

        processed_files = []
        for processed_file_list in processed_file_lists:
            processed_files.extend([file.name for file in processed_file_list])
        
        unique_processed_files = list(set(processed_files))

        for processed_file in tqdm(unique_processed_files):
            dfs = []
            for country_config in country_configs:
                try:
                    df = pd.read_csv(country_config.get_processed_data_path() / processed_file)
                    dfs.append(df)
                except FileNotFoundError:
                    pass
            
            if len(dfs) > 0:
                df = pd.concat(dfs)
                df.to_csv(self.config.get_consolidated_data_path() / processed_file, index=False)
