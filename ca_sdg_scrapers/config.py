from abc import ABC
from typing import Union
from pathlib import Path
import yaml

class Configurable(ABC):
    def __init__(self, country_id: str, path_to_config: Union[str, Path, None] = None):
        self.config = Config(path_to_config, country_id=country_id)
        self.country_id = country_id
        self.common_params: dict = self.config.get_common_params()
        self.country_params: dict = self.config.get_country_params()
        self.country_code = self.country_params['M49_country_code']    

def country_id_required(func):
    def wrapper(self, *args, **kwargs):
        if self.country_id is None:
            raise ValueError('Country ID must be specified.')
        return func(self, *args, **kwargs)
    return wrapper

class Config(object):
    def __init__(self, path_to_config: Union[str, Path, None] = None, country_id: Union[str, None] = None) -> None:
        self.path_to_config = path_to_config
        self.country_id = country_id

        if path_to_config is None:
            path_to_config = str(Path.cwd() / 'config.yaml')

        with open(path_to_config) as f:
            self.params = yaml.safe_load(f)
        
        self.common = self.get_common_params()

        if country_id is not None:
            if country_id not in self.params['country_specific']:
                raise ValueError(f'Unsupported country {country_id}, supported countries are: {list(self.params["country_specific"].keys())}')
            self.country_config = self.get_country_params()

    @country_id_required
    def get_country_params(self) -> dict:
        """Get country-specific parameters."""
        return self.params['country_specific'][self.country_id]
    
    def get_common_params(self) -> dict:
        """Get common parameters."""
        return self.params['common']

    def get_output_path(self) -> Path:
        """Get path to save data to."""
        if self.common.get('output_folder') is not None:
            path = Path(self.common['output_folder'])
        else:
            path = Path.cwd()
        
        if self.common['create_folders']:
            path.mkdir(parents=True, exist_ok=True)

        return path

    @country_id_required
    def get_raw_data_path(self) -> Path:
        """Get path to save raw data to."""
        path = self.get_output_path() / self.common['raw_data_folder'] / self.country_config['folder_name']

        if self.common['create_folders']:
            path.mkdir(parents=True, exist_ok=True)

        return path

    @country_id_required
    def get_processed_data_path(self) -> Path:
        """Get path to save processed data to."""
        path = self.get_output_path() / self.common['processed_data_folder'] / self.country_config['folder_name']

        if self.common['create_folders']:
            path.mkdir(parents=True, exist_ok=True)

        return path
    
    @country_id_required
    def get_metadata_path(self) -> Path:
        """Get path to save metadata to."""
        path = self.get_output_path() / self.common['metadata_folder'] / self.country_config['folder_name']

        if self.common['create_folders']:
            path.mkdir(parents=True, exist_ok=True)

        return path
    
    def get_consolidated_data_path(self) -> Path:
        """Get path to save consolidated data to."""
        path = self.get_output_path() / self.common['consolidated_data_folder']

        if self.common['create_folders']:
            path.mkdir(parents=True, exist_ok=True)

        return path
    
    @country_id_required
    def get_raw_file_list(self) -> list[Path]:
        """Get list of raw files from the source."""
        return [f for f in self.get_raw_data_path().iterdir() if f.is_file()]

    @country_id_required
    def get_processed_file_list(self) -> list[Path]:
        """Get list of processed files from the source."""
        return [f for f in self.get_processed_data_path().iterdir() if f.is_file()]
