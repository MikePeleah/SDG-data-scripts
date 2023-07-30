from sdg_transformers import Transformer
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--country-codes', nargs='+', type=str, required=True, help='Country codes to get data for')
    parser.add_argument('--path-to-config', help='Path to config file')
    args = parser.parse_args()

    for country_code in args.country_codes:
        transformer = Transformer.get_transformer(country_code, path_to_config=args.path_to_config)

        print(f'Transforming data for {country_code}')
        transformer.transform_data()
