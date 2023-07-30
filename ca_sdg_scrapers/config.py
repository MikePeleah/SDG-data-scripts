from typing import Union
from pathlib import Path
import yaml

class Config(object):
    def __init__(self, path_to_config: Union[str, Path, None], country_id: Union[str, None] = None) -> None:
        self.path_to_config = path_to_config
        self.country_id = country_id

        if path_to_config is None:
            path_to_config = str(Path.cwd() / 'config.yaml')

        with open(path_to_config) as f:
            self.params = yaml.safe_load(f)
        
        self.common = self.get_common_params()

        if country_id is not None:
            self.country_config = self.get_country_params(country_id)

    def get_country_params(self, country_id) -> dict:
        if country_id not in self.params['country_specific']:
            raise ValueError(f'Unsupported country {country_id}, supported countries are: {list(self.params["country_specific"].keys())}')
        return self.params['country_specific'][country_id]
    
    def get_common_params(self) -> dict:
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

    def get_raw_data_path(self) -> Path:
        """Get path to save raw data to."""
        if self.country_id is None:
            raise ValueError('Country ID must be specified.')

        path = self.get_output_path() / self.common['raw_data_folder'] / self.country_config['folder_name']

        if self.common['create_folders']:
            path.mkdir(parents=True, exist_ok=True)

        return path

    def get_processed_data_path(self) -> Path:
        """Get path to save processed data to."""
        if self.country_id is None:
            raise ValueError('Country ID must be specified.')

        path = self.get_output_path() / self.common['processed_data_folder'] / self.country_config['folder_name']

        if self.common['create_folders']:
            path.mkdir(parents=True, exist_ok=True)

        return path
    
    def get_metadata_path(self) -> Path:
        """Get path to save metadata to."""
        if self.country_id is None:
            raise ValueError('Country ID must be specified.')

        path = self.get_output_path() / self.common['metadata_folder'] / self.country_config['folder_name']

        if self.common['create_folders']:
            path.mkdir(parents=True, exist_ok=True)

        return path
    
    def get_consolidated_data_path(self) -> Path:
        """Get path to save consolidated data to."""
        if self.country_id is None:
            raise ValueError('Country ID must be specified.')

        path = self.get_output_path() / self.common['consolidated_data_folder']

        if self.common['create_folders']:
            path.mkdir(parents=True, exist_ok=True)

        return path
