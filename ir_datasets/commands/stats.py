import pkgutil
import json
import argparse
import ir_datasets


_logger = ir_datasets.log.easy()


def count_downloads(data, key='url'):
    if isinstance(data, dict):
        if key in data:
            return 1
        return sum(count_downloads(v, key=key) for v in data.values())
    return 0


def count_metadata(data, etype):
    return sum(r.get(etype, {}).get('count', 0) for r in data.values())


def main(args):
    parser = argparse.ArgumentParser(prog='ir_datasets stats', description='Provides statistics about ir_datasets')
    args = parser.parse_args(args)

    directly_registered_count = len(ir_datasets.registry._registered)

    downloads = json.loads(pkgutil.get_data('ir_datasets', 'etc/downloads.json'))
    download_count = count_downloads(downloads)
    mirror_count = count_downloads(downloads, key='irds_mirror')

    print(f'datasets & subsets: {directly_registered_count}+')
    print(f'downloadable files: {download_count}+')
    print(f'mirrored files: {mirror_count}')


if __name__ == '__main__':
    main(sys.argv[1:])
