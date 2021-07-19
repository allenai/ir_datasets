import sys
import os
import argparse
import multiprocessing
from collections import deque
import ir_datasets
from ir_datasets.util import DownloadConfig


RED = '\u001b[31m'
RES = '\u001b[0m'


_logger = ir_datasets.log.easy()

def walk_path(start_path='.', skips=[]):
    # adapted from <https://stackoverflow.com/a/1392549/406914>
    total_size = 0
    files = []
    for dirpath, dirnames, filenames in os.walk(start_path):
        if any(s for s in skips if dirpath.startswith(s)):
            continue
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if fp in skips:
                continue
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
                files.append(fp)
    return total_size, files


def clean(dataset, yes=False, list=False, human=True):
    base_path = os.path.join(ir_datasets.util.home_path()/dataset)
    dlc = DownloadConfig.context(dataset, base_path)
    skips = []
    for dl_item in dlc.contents().values():
        if 'instructions' in dl_item and 'cache_path' in dl_item: # non-downloadble item
            skips.append(os.path.join(base_path, dl_item['cache_path']))
    size, files = walk_path(base_path, skips)
    files_fmt = f'{len(files)} files'
    if human:
        size_fmt = ir_datasets.util.format_file_size(size)
        if size > 1_000_000_000: # sizes over 1GB: list in red
            size_fmt = f'{RED}{size_fmt}{RES}'
    else:
        size_fmt = str(size)
    if list:
        if size > 0:
            print(f'{size_fmt}\t{files_fmt}\t{dataset}')
        return
    if not yes:
        inp = None
        while inp not in ('y', 'yes'):
            inp = input(f'clean up {size_fmt} from {dataset} ({files_fmt})?\n[y(es) / n(o) / l(ist files)] ').lower()
            if inp in ('l', 'list', 'list files'):
                for file in files:
                    f_size = os.path.getsize(file)
                    if human:
                        fsize_fmt = ir_datasets.util.format_file_size(f_size)
                        if f_size > 1_000_000_000: # sizes over 1GB: list in red
                            fsize_fmt = f'{RED}{fsize_fmt}{RES}'
                    else:
                        fsize_fmt = str(size)
                    print(f'{fsize_fmt}\t{file}')
            if inp in ('n', 'no'):
                return

    # remove identified files
    for file in files:
        os.remove(file)

    # remove empty directories
    for dirpath, dirnames, filenames in os.walk(base_path, topdown=False):
        if not dirnames and not filenames:
            os.rmdir(dirpath)


def main(args):
    parser = argparse.ArgumentParser(prog='ir_datasets clean', description='Cleans up space by removing files that can automatically be rec-reated or re-downloaded.')
    parser.add_argument('datasets', nargs='*', help='dataset IDs to clean up')
    parser.add_argument('--yes', '-y', action='store_true', help='automatically say yes to confirmation messages')
    parser.add_argument('--list', '-l', action='store_true', help='lists datasets available for cleanup and their sizes; does not do any cleanup')
    parser.add_argument('-H', action='store_false', help='output raw sizes, rather than human-readable versions')

    args = parser.parse_args(args)
    try:
        if args.datasets:
            top_level_datasets = {d for d in ir_datasets.registry._registered if '/' not in d}
            for dataset in args.datasets:
                if dataset not in top_level_datasets:
                    print(f'Skipping unknown dataset {dataset}')
                else:
                    clean(dataset, args.yes, list=args.list, human=args.H)
        elif args.list:
            for dataset in ir_datasets.registry._registered:
                if '/' not in dataset:
                    clean(dataset, list=True, human=args.H)
        else:
            sys.stderr.write('ERROR: Please provide either --list, dataset IDs to clean, or --help for more details\n')
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main(sys.argv[1:])
