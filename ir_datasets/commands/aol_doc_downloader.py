import requests
from nltk import word_tokenize
import contextlib
import fcntl
import time
import multiprocessing
import os
import sys
from pathlib import Path
import gzip
import hashlib
import json
import pickle
import argparse
import ir_datasets
import io
import re
import chardet
import codecs
from lxml.html import etree
import ir_datasets
from ir_datasets import util


def sax_html_parser(body):
    sax = SaxExtractor()
    parser = etree.HTMLParser(target=sax)
    if isinstance(body, bytes):
        encoding = chardet.detect(body)['encoding'] or 'utf8'
        cdc = codecs.lookup(encoding)
        while body:
            text, count = cdc.decode(body, 'ignore')
            parser.feed(text)
            body = body[count:]
    else:
        parser.feed(body)
    parser.close()
    return sax.get_title(), str(sax)

class SaxExtractor:
    IGNORE_TAGS = {'noscript', 'meta', 'input', 'script', 'style'}
    def __init__(self):
        self.title = io.StringIO()
        self.text = io.StringIO()
        self.in_title = False
        self.ignore_tag_stack = []
    def __str__(self):
        self.text.seek(0)
        return self.text.read()
    def get_title(self):
        self.title.seek(0)
        return self.title.read()
    def data(self, data):
        if not self.ignore_tag_stack:
            if self.in_title:
                self.title.write(data)
            else:
                self.text.write(data)
    def start(self, tag, attrs):
        tag = tag.lower()
        if tag.lower() in self.IGNORE_TAGS:
            self.ignore_tag_stack.append(tag)
        if tag.lower() == 'title':
            self.in_title = True
    def end(self, tag):
        tag = tag.lower()
        if tag in self.IGNORE_TAGS:
            while self.ignore_tag_stack and self.ignore_tag_stack.pop() != tag:
                pass
        if tag.lower() == 'title':
            self.in_title = False
    def close(self):
        pass
    def comment(self, data):
        pass
    def doctype(self, *args):
        pass
    def pi(self, *args):
        pass


_logger = ir_datasets.log.easy()

_session = None
def start():
    global _session
    _session = requests.Session()

def worker(args):
    global _session
    frame = ir_datasets.lazy_libs.lz4_frame().frame
    docid, (url, wb_url) = args
    # I've seen that in rare situations, data can get mangled by intermediate parties if we use
    # the http endpoint instead of the https endpoint.
    wb_url = wb_url.replace('http://web.archive.org/web', 'https://web.archive.org/web')
    try:
        resp = _session.get(wb_url, stream=True, timeout=15)
        resp.raise_for_status()
        if 'html' not in resp.headers.get('content-type', '').lower():
            raise ValueError(f'content-type {resp.headers.get("content-type")}')
        resp.raw.decode_content = True
        raw_body = resp.raw.read()
        title, text = sax_html_parser(raw_body)
        title = ' '.join(word_tokenize(title))
        text = ' '.join(word_tokenize(text))
        if title == '502 Bad Gateway':
            # This isn't raised by raise_for_status for some reason, but it means archive.org is booting us
            # There's a few exceptions where this is actually the expected title, don't raise for those.
            if not (docid in ('80445ed4fc45', '9ff7c85c8c28', '03e460cf3fa1') and text == '502 Bad Gateway nginx'):
                raise RuntimeError('502 bad gateway')
        with (util.home_path()/'aol'/'tmp_docs'/f'{docid[0]}.jsonl.lz4').open('ab') as fout:
            try:
                fcntl.lockf(fout, fcntl.LOCK_EX)
                fout.seek(0, 2) # seek to the end
                fout.write(frame.compress(json.dumps({
                    'doc_id': docid,
                    'url': url,
                    'wb_url': wb_url,
                    'title': title,
                    'text': text,
                }).encode() + b'\n'))
            finally:
                fcntl.lockf(fout, fcntl.LOCK_UN)
        return docid, None
    except Exception as ex:
        ex = str(ex)
        if '[Errno 111] Connection refused' in ex:
            ex = 'connection refused'
        elif 'Read timed out' in ex:
            ex = 'read timed out'
        return docid, f'failed download of {wb_url}: {ex}; will retry'


def main(args):
    parser = argparse.ArgumentParser(prog='ir_datasets aol_doc_downloader', description='Downloads documents for the AOL dataset from archive.org')
    parser.add_argument('--parallel', default=10, type=int)
    parser.add_argument('--backoff_threshold', default=10, type=int)
    parser.add_argument('--backoff_duration', default=10., type=float)
    args = parser.parse_args(args)
    assert 1 <= args.parallel
    success_ids = set()
    todo = set()
    in_progress = set()
    with _logger.duration('preparing to download...'):
        if not (util.home_path()/'aol'/'tmp_docs').exists():
            (util.home_path()/'aol'/'tmp_docs').mkdir(exist_ok=True, parents=True)
        if (util.home_path()/'aol'/'success_ids.txt').exists():
            with (util.home_path()/'aol'/'success_ids.txt').open('rt') as fin:
                for line in fin:
                    success_ids.add(line.strip())
        did2url = {}
        aol_manager = ir_datasets.datasets.aol.manager
        with aol_manager.id2wb_dlc.stream() as fin:
            for line in fin:
                did, url, wb_url = line.decode().rstrip('\n').split('\t')
                did2url[did] = (url, wb_url)
                if did not in success_ids:
                    todo.add(did)
    total_reqests = 0
    total_success = 0
    start_time = time.time()
    with _logger.pbar_raw(desc='downloading aol docs', unit='requests') as pbar:
        try:
            while todo:
                failures_in_a_row = 0
                with contextlib.ExitStack() as stack:
                    f_success = stack.enter_context((util.home_path()/'aol'/'success_ids.txt').open('at'))
                    fcntl.lockf(f_success, fcntl.LOCK_EX)
                    def doc_iter():
                        while todo or in_progress:
                            if todo:
                                did = todo.pop()
                                in_progress.add(did)
                                yield did, did2url[did]
                            else:
                                time.sleep(1.) # wait 'till remaining items in_progress succeed (exiting loop) or fail (which moves it to todo)
                    if args.parallel == 1:
                        _logger.warn('starting with 1 process; consider setting --parallel with a higher value to speed up download process.')
                        start()
                        mapper = map
                    else:
                        pool = stack.enter_context(multiprocessing.Pool(args.parallel, start))
                        mapper = pool.imap_unordered
                    for docid, error in mapper(worker, doc_iter()):
                        if error is None:
                            f_success.write(f'{docid}\n')
                            success_ids.add(docid)
                            failures_in_a_row = 0
                            total_success += 1
                        else:
                            _logger.info(error)
                            todo.add(docid)
                            failures_in_a_row += 1
                            if failures_in_a_row >= args.backoff_threshold:
                                pool.terminate()
                                todo.update(in_progress)
                                in_progress.clear()
                                _logger.info(f'{failures_in_a_row} failures in a row; backing off for {args.backoff_duration}sec...')
                                time.sleep(args.backoff_duration)
                                break
                        in_progress.discard(docid)
                        total_reqests += 1
                        overall_rate = total_reqests / (time.time() - start_time)
                        est_remaining_requests = (len(todo) + len(in_progress)) / (total_success/total_reqests)
                        est_remaining_time = est_remaining_requests / overall_rate
                        pbar.set_postfix({
                            'todo': str(len(todo) + len(in_progress)),
                            'done': str(len(success_ids)),
                            'done%': '{:.2f}%'.format(len(success_ids)/len(did2url)*100),
                            'suc_rate': '{:.1f}%'.format(total_success/total_reqests*100),
                            'est_remaining': ir_datasets.log.format_interval(est_remaining_time),
                        }, refresh=False)
                        pbar.update()
        except KeyboardInterrupt:
            _logger.info('KeyboardInterrupt')


if __name__ == '__main__':
    main(sys.argv[1:])
