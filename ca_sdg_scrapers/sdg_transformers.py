from typing_extensions import Self
from typing import Union
from pathlib import Path
from tqdm import tqdm
import pandas as pd
import sys
from config import Config, Configurable

class Transformer(Configurable):
    def __init__(self, country_id: str, path_to_config: Union[str, Path, None] = None):
        super().__init__(country_id, path_to_config)

    @classmethod
    def get_transformer(cls, country_id: str, path_to_config: Union[str, Path, None] = None, *args, **kwargs) -> Self:
        """Get transformer for a specific country code."""
        config = Config(path_to_config)
        country_params = config.get_country_params(country_id)

        transformer = getattr(sys.modules[__name__], country_params['transformer'])
        return transformer(country_id, path_to_config=path_to_config, *args, **kwargs)

    def transform_data(self):
        """Get data from the source and save it as CSV files."""
        raw_files = self.config.get_raw_file_list()

        for raw_file in tqdm(raw_files):
            df = pd.read_csv(raw_file)
            df['M49_country_code'] = self.country_code
            df.to_csv(self.config.get_processed_data_path() / raw_file.name, index=False)

class KAZTransformer(Transformer):
    pass

class KGZTransformer(Transformer):
    pass

class UZBTransformer(Transformer):
    pass
