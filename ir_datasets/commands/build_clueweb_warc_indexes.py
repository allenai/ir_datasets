import sys
import multiprocessing
from pathlib import Path
import argparse
import ir_datasets


_logger = ir_datasets.log.easy()


def process(args):
    source_file, output_file, cw09 = args
    index = ir_datasets.indices.ClueWebWarcIndex(str(source_file), str(output_file), warc_cw09=cw09)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if not index.built():
        index.build()
    return source_file


def main(args):
    parser = argparse.ArgumentParser(prog='ir_datasets build_clueweb_warc_indexes', description='Buildes indexes for ClueWeb WARC files.')
    parser.add_argument('source_dir')
    parser.add_argument('output_dir')
    parser.add_argument('--processes', default=1, type=int)
    parser.add_argument('--cw09', action='store_true')
    args = parser.parse_args(args)
    source_dir = Path(args.source_dir)
    output_dir = Path(args.output_dir)
    all_source_files = [f.relative_to(source_dir) for f in source_dir.rglob('*.warc.gz')]
    all_source_files = sorted(all_source_files)
    process_args = [(source_dir/f, output_dir/f'{f}.chk.lz4', args.cw09) for f in all_source_files]
    process_args = [a for a in process_args if not a[1].exists()]
    with _logger.pbar_raw(total=len(process_args)) as pbar:
        if args.processes == 1:
            for src in map(process, process_args):
                pbar.update(1)
                pbar.set_postfix(file=str(src))
        else:
            with multiprocessing.Pool(args.processes) as pool:
                for src in pool.imap_unordered(process, process_args):
                    pbar.update(1)
                    pbar.set_postfix(file=src.relative_to(source_dir))


if __name__ == '__main__':
    main(sys.argv[1:])
