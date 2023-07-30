from abc import ABC, abstractmethod
from typing_extensions import Self
from typing import Union
from pathlib import Path
import requests
from tqdm import tqdm
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from utils import download_file
import sys
from config import Config, Configurable

class Scraper(Configurable):
    def __init__(self, country_id: str, path_to_config: Union[str, Path, None] = None):
        super().__init__(country_id, path_to_config)

    @classmethod
    def get_scraper(cls, country_id: str, path_to_config: Union[str, Path, None] = None, *args, **kwargs) -> Self:
        """Get scraper for a specific country code."""
        config = Config(path_to_config=path_to_config, country_id=country_id)
        country_params = config.get_country_params()

        scraper = getattr(sys.modules[__name__], country_params['scraper'])
        return scraper(country_id, path_to_config=path_to_config, *args, **kwargs)

    @abstractmethod
    def get_data(self):
        """Get data from the source and save it as CSV files."""
        pass

    @abstractmethod
    def get_metadata(self):
        """Get metadata from the source and save it as CSV files."""
        pass


class UZBScraper(Scraper):
    def __init__(self, country_id: str, path_to_config: Union[str, Path, None] = None):
        super().__init__(country_id, path_to_config)

    def _get_indicator_ids(self) -> list:
        """Get indicator ids from the source."""
        available_indicators = []

        for goal_id in tqdm(range(17)):
            r = requests.get(f'https://nsdg.stat.uz/api/databanks/get-data-banks/?id={goal_id+1}&lang=en')
            indicators = r.json()['all_indicators']
            for indicator in indicators:
                if indicator['val_exist'] == 1:
                    available_indicators.append(indicator['indicator_id'])

        return available_indicators
    
    def _get_indicator_data(self, indicator: str) -> Union[pd.DataFrame, None]:
            """Get indicator data from the source."""
            r = requests.get(f'https://nsdg.stat.uz/api/databanks/get-indicator-table/?id={indicator}&lang=en')

            try:
                json_response = r.json()
            except requests.JSONDecodeError:
                return None

            columns = pd.DataFrame(json_response['tableColumns'])
            values = pd.DataFrame(json_response['tblValue'])

            df = pd.merge(columns, values, on='unique_id')

            return df

    def get_data(self):
        """Get data from the source and save it as CSV files."""
        available_indicators = []

        print('Getting indicator ids')

        available_indicators = self._get_indicator_ids()

        print('Getting indicator data')

        for indicator in tqdm(available_indicators):
            df = self._get_indicator_data(indicator)

            if df is not None:
                save_to_path = self.config.get_raw_data_path() / (indicator.replace('.', '-') + '.csv')
                df.to_csv(save_to_path, index=False)
    
    def get_metadata(self):
        raise NotImplementedError


class OpenSDGScraper(Scraper):
    def __init__(self, country_id: str, path_to_config: Union[str, Path, None] = None, *args, **kwargs):
        super().__init__(country_id, path_to_config, *args, **kwargs)
        self.base_url = self.country_params['base_url']
        self.goal_page_base_url = self.country_params['goal_page_base_url']
        self.indicator_link_element = self.country_params['indicator_link_element']
        self.indicator_link_class = self.country_params['indicator_link_class']

    def _get_indicator_page_urls(self) -> list:
        """Get indicator page URLs from the source."""
        indicator_page_urls = []

        for goal_id in tqdm(range(17)):
            r = requests.get(self.goal_page_base_url.format(str(goal_id + 1)))
            soup = BeautifulSoup(r.text, 'html.parser')
            for link in soup.find_all(self.indicator_link_element, class_=self.indicator_link_class):
                indicator_page_urls.append(link.a['href'])

        return indicator_page_urls
    
    def _get_download_urls(self, indicator_page_urls: list) -> list:
        """Get CSV download URLs from the source."""
        download_urls = []

        for indicator_page_url in tqdm(indicator_page_urls):
            r = requests.get(self.base_url + indicator_page_url)
            soup = BeautifulSoup(r.text, 'html.parser')
            download_links = soup.find_all('a', string='Download Source CSV')
            if len(download_links) > 0:
                download_urls.append(download_links[0].get('href'))

        return download_urls

    def _download_indicators(self, download_urls: list):
        """Download CSV files from the source."""
        for download_url in tqdm(download_urls):
            filename = Path(urlparse(download_url).path).name
            save_to_path = self.config.get_raw_data_path() / filename
            download_file(download_url, save_to_path)

    def get_data(self):
        """Get data from the source and save it as CSV files."""
        print('Getting indicator page URLs')

        indicator_page_urls = self._get_indicator_page_urls()

        print('Getting CSV download URLs')

        download_urls = self._get_download_urls(indicator_page_urls)

        print('Downloading CSV files')

        self._download_indicators(download_urls)

    def get_metadata(self):
        raise NotImplementedError

