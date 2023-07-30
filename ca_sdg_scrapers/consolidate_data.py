from sdg_consolidator import Consolidator
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--country-codes', nargs='+', type=str, required=True, help='Country codes to get data for')
    parser.add_argument('--path-to-config', help='Path to config file')
    args = parser.parse_args()

    consolidator = Consolidator(path_to_config=args.path_to_config)

    print(f'Consolidating data for {args.country_codes}')
    consolidator.consolidate_data(args.country_codes)
