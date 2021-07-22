import os
import sys
import multiprocessing
from pathlib import Path
import gzip
import hashlib
import json
import pickle
import argparse
import ir_datasets


_logger = ir_datasets.log.easy()


def process(args):
    lz4 = ir_datasets.lazy_libs.lz4_frame()
    source_file, output_file = args
    checkpoint_data = []
    with ir_datasets.lazy_libs.zlib_state().GzipStateFile(str(source_file), keep_last_state=True) as f, _logger.pbar_raw(desc='building checkpoint') as pbar:
        idx = 0
        while not f.eof():
            if idx % 1500 == 0:
                state, pos = f.last_state, f.last_state_pos
                offset = f.output_pos - f.last_state_output_pos
                checkpoint_data.append((pos, state, offset))
            f.readline()
            idx += 1
            pbar.update(1)
    with lz4.frame.LZ4FrameFile(output_file, mode='a', block_linked=True, compression_level=lz4.frame.COMPRESSIONLEVEL_MAX, auto_flush=True) as fout:
        pickle.dump(checkpoint_data, fout)
    return source_file


def main(args):
    parser = argparse.ArgumentParser(prog='ir_datasets build_c4_checkpoints', description='Buildes gzip checkpoint files for C4 documents.')
    parser.add_argument('source_dir')
    parser.add_argument('output_dir')
    parser.add_argument('--skip_last', action='store_true')
    parser.add_argument('--sources_file')
    args = parser.parse_args(args)
    source_dir = Path(args.source_dir)
    output_dir = Path(args.output_dir)
    all_source_files = source_dir.rglob('*.json.gz')
    all_source_files = sorted(all_source_files)
    if args.sources_file:
        sources = []
        for file in _logger.pbar(all_source_files, desc='building sources file', unit='file'):
            try:
                count = 0
                with gzip.open(file, 'rb') as f:
                    for line in f:
                        count += 1
                h = hashlib.new('md5')
                h.update(open(file, 'rb').read())
                md5 = h.hexdigest().lower()
                size = os.path.getsize(file)
                sources.append({
                    "name": f"en.noclean.{file.name}",
                    "url": f"https://huggingface.co/datasets/allenai/c4/resolve/main/en.noclean/{file.name}",
                    "expected_md5": md5,
                    "size_hint": size,
                    "checkpoint_freq": 1500,
                    "doc_count": count,
                })
            except Exception as ex:
                print(file, ex)
        with gzip.open(args.sources_file + '.gz', 'wt') as f:
            json.dump(sources, f)
    all_source_files = [f.relative_to(source_dir) for f in all_source_files]
    if args.skip_last:
        all_source_files = all_source_files[:-1]
    process_args = [(source_dir/f, output_dir/f'{f}.chk.pkl.lz4') for f in all_source_files]
    process_args = [a for a in process_args if not a[1].exists()]
    with _logger.pbar_raw(total=len(process_args), unit='file') as pbar:
        for src in map(process, process_args):
            pbar.update(1)
            pbar.set_postfix(file=str(src)[-20:])


if __name__ == '__main__':
    main(sys.argv[1:])
