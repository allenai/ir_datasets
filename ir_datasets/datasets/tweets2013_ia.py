import os
import itertools
import contextlib
import shutil
import tarfile
from collections import Counter
from pathlib import Path
import bz2
import json
from datetime import datetime
from typing import NamedTuple
import ir_datasets
from ir_datasets.util import DownloadConfig
from ir_datasets.formats import TrecQrels, TrecQueries, BaseDocs
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.indices import Docstore, CacheDocstore


NAME = 'tweets2013-ia'
_logger = ir_datasets.log.easy()


QTYPE_MAP_13 = {
    '<num> *Number: *MB': 'query_id', # Remove MB prefix from QIDs
    '<query> *': 'query',
    '<querytime> *': 'time',
    '<querytweettime> *': 'tweet_time',
}


QTYPE_MAP_14 = {
    '<num> *Number: *MB': 'query_id', # Remove MB prefix from QIDs
    '<query> *': 'query',
    '<querytime> *': 'time',
    '<querytweettime> *': 'tweet_time',
    '<querydescription> *': 'description',
}

RM_TAGS = [' </num>', ' </query>', ' </querytime>', ' </querytweettime>']


QREL_DEFS = {
    2: 'highly relevant',
    1: 'relevant',
    0: 'not relevant',
}


class TweetDoc(NamedTuple):
    doc_id: str
    text: str
    user_id: str
    created_at: str
    lang: str
    reply_doc_id: str
    retweet_doc_id: str
    source: bytes
    source_content_type: str


class TrecMb13Query(NamedTuple):
    query_id: str
    query: str
    time: str
    tweet_time: str


class TrecMb14Query(NamedTuple):
    query_id: str
    query: str
    time: str
    tweet_time: str
    description: str


"""
About the tweets2013-ia collection:

This collection uses tweets distributed by the Internet Archive. These archives are not /exactly/ the same
that were used for the TREC Microblog 2013--14 shared tasks, but Sequiera and Lin [1] show that it's close enough.

The distribution format splits up the tweets by the date/time posted, like: MM/DD/hh/mm.bz4. Since the "snowflake"
format of these tweet IDs include a timestamp, you'd think that this would make performing lookups from this structure
easy, but it turns out that there's plenty of cases where tweets are in the wrong files. I suspect it may have to do
with when the tweets were streamed, not when they were actually created.

To make lookups efficient, we re-create the strucutre with a few changes. First, we allocate tweets to files based
on the timestamp in their tweet IDs, allowing the ID itself to point to the source file. In the process, we change
the compression technique from using bz2 compression to lz4, allowing much faster decompression. lz4 also allows us
to append to compressed files, so we can add to files as tweets are encounterd from other source files. At the same
time, we add some optimizations to allow for faster lookups in these files. For tweets belonging to the same file
are encountered sequentially, they are batched up before writing. These batches are sorted and then split into groups
of at most 100 tweets. Each group is precided by a short JSON record containing the start and end tweets IDs in the
group. Since these recrods are much shorter than regular tweet records, they can be identified without parsing the
JSON. If the target tweet does not appear in this range, we can skip JSON parsing of all recrods until the next short
one. My benchmarks showed this speeds up lookups by at most ~5x. The uncompressed files end up looking like this:

{"start": 102, "end": 145}
{"id": 102, "tweet": "[text]", ...}
{"id": 124, "tweet": "[text]", ...}
{"id": 125, "tweet": "[text]", ...}
{"id": 145, "tweet": "[text]", ...}
{"start": 163, "end": 341}
{"id": 163, "tweet": "[text]", ...}
...

Because of tweets encountered in other files, it's not uncommon to see ranges with only 1 or 2 tweets at the start or
end of these files. Note that the entire file is not sorted; this would take a second pass which isn't really needed.
Most files have at most a few thousand tweets. We considered a different granularity for these files (by hour? by 5min?)
but the by-minute structure seemed reasonable enough.

Downsides with this approach are:
 a) We're not loading directly from source anymore
 b) The source looks similar to this structure, which may lead to confusion.

Alternative approachs would have been to:
 1) Build an pklz4 docstore for this collection. Downsides with that are:
    a) It would have a huge docid lookup table (several GB). Would need to keep this all in memory when indexing.
    b) No compression between records, so larger file size
 2) Store pklz4 in this structure, rather than the source JSON. This would be faster, but downsides there are:
    a) If we ever want to change the fields in TweetDoc, we'd need to rebuild the whole structure
    b) Not a human-readable format (i.e., you couldn't use lz4cat to see the contents of these files)

[1] https://cs.uwaterloo.ca/~jimmylin/publications/Sequiera_Lin_SIGIR2017.pdf
"""


class TweetWriter:
    def __init__(self, base_path, max_tweets_per_block=100):
        self.base_path = Path(base_path)
        self.current_file = None
        self.buffered_tweets = []
        self.max_tweets_per_block = max_tweets_per_block

    def add(self, file_name, tweet_id, tweet_data):
        if file_name != self.current_file:
            self.flush()
            self.current_file = file_name
        self.buffered_tweets.append((tweet_id, tweet_data))

    def flush(self):
        lz4 = ir_datasets.lazy_libs.lz4_frame()
        if self.current_file is not None and self.buffered_tweets:
            (self.base_path / Path(self.current_file)).parent.mkdir(parents=True, exist_ok=True)
            with lz4.frame.LZ4FrameFile(self.base_path / self.current_file, mode='a', block_linked=True, compression_level=lz4.frame.COMPRESSIONLEVEL_MAX, auto_flush=True) as fout:
                sorted_tweets = sorted(self.buffered_tweets)
                while sorted_tweets:
                    block = sorted_tweets[:self.max_tweets_per_block]
                    sorted_tweets = sorted_tweets[self.max_tweets_per_block:]
                    header = json.dumps({'start': block[0][0], 'end': block[-1][0]}).encode() + b'\n'
                    fout.write(b''.join([header] + [t[1] for t in block]))
        self.current_file = None
        self.buffered_tweets.clear()


class Tweets2013IaDocIter:
    def __init__(self, tweets_docs, slice):
        self.tweets_docs = tweets_docs
        self.slice = slice
        self.next_index = 0
        self.file_iter = tweets_docs._docs_iter_source_files()
        self.current_file = None
        self.current_file_start_idx = 0
        self.current_file_end_idx = 0

    def __next__(self):
        if self.slice.start >= self.slice.stop:
            raise StopIteration
        while self.next_index != self.slice.start or self.current_file is None or self.current_file_end_idx <= self.slice.start:
            if self.current_file is None or self.current_file_end_idx <= self.slice.start:
                # First iteration or no docs remaining in this file
                if self.current_file is not None:
                    self.current_file.close()
                    self.current_file = None
                # jump ahead to the file that contains the desired index
                first = True
                while first or self.current_file_end_idx < self.slice.start:
                    source_file = next(self.file_iter)
                    self.next_index = self.current_file_end_idx
                    self.current_file_start_idx = self.current_file_end_idx
                    self.current_file_end_idx = self.current_file_start_idx + self.tweets_docs._docs_file_counts()[source_file]
                    first = False
                self.current_file = self.tweets_docs._docs_ctxt_iter_tweets(source_file)
            else:
                for _ in zip(range(self.slice.start - self.next_index), self.current_file):
                    # The zip here will stop at after either as many docs we must advance, or however
                    # many docs remain in the file. In the latter case, we'll just drop out into the
                    # next iteration of the while loop and pick up the next file.
                    self.next_index += 1
        result = next(self.current_file)
        self.next_index += 1
        self.slice = slice(self.slice.start + (self.slice.step or 1), self.slice.stop, self.slice.step)
        return result

    def close(self):
        self.file_iter = None
        self.current_file = None

    def __iter__(self):
        return self

    def __del__(self):
        self.close()

    def __getitem__(self, key):
        if isinstance(key, slice):
            # it[start:stop:step]
            new_slice = ir_datasets.util.apply_sub_slice(self.slice, key)
            return Tweets2013IaDocIter(self.tweets_docs, new_slice)
        elif isinstance(key, int):
            # it[index]
            new_slice = ir_datasets.util.slice_idx(self.slice, key)
            new_it = Tweets2013IaDocIter(self.tweets_docs, new_slice)
            try:
                return next(new_it)
            except StopIteration as e:
                raise IndexError((self.slice, slice(key, key+1), new_slice))
        raise TypeError('key must be int or slice')


class TweetsDocstore(Docstore):
    def __init__(self, tweets_docs):
        super().__init__(tweets_docs.docs_cls(), 'doc_id')
        self.tweets_docs = tweets_docs

    def get_many_iter(self, doc_ids):
        lz4 = ir_datasets.lazy_libs.lz4_frame()
        files_to_search = {}

        # find the file that each tweet should be found in
        for doc_id in doc_ids:
            try:
                doc_id = int(doc_id)
            except ValueError:
                continue # tweet IDs must be ints, so skip this one
            source_file = self.tweets_docs._id2file(doc_id)
            if source_file not in files_to_search:
                files_to_search[source_file] = set()
            files_to_search[source_file].add(doc_id)

        # loop through each required source file to find the tweets
        for source_file, doc_ids in files_to_search.items():
            if not (Path(self.tweets_docs.docs_path()) / source_file).exists():
                continue # source file missing
            with lz4.frame.LZ4FrameFile(Path(self.tweets_docs.docs_path()) / source_file) as fin:
                block_docids = set()
                line_iter = iter(fin)
                while fin:
                    line = next(fin, StopIteration)
                    if line is StopIteration:
                        break # bummer, can't find a doc_id...
                    if len(line) < 64: # checkpoints lines can be at most ~60 characters. Tweets lines always be longer than this.
                        # It's a checkpoint line! Are we looking for anything in this range?
                        rng = json.loads(line)
                        start, end = rng['start'], rng['end']
                        block_docids = set(d for d in doc_ids if start <= d <= end)
                    elif block_docids:
                        # Is this record a tweet we're looking for?
                        data = json.loads(line)
                        if data['id'] in block_docids:
                            yield self.tweets_docs._docs_source_to_doc(line, data)
                            block_docids.discard(data['id'])
                            doc_ids.discard(data['id'])
                            if not doc_ids:
                                break # all done with this file
                    else:
                        # None of the docs we're looking for are in this block, so we don't need to bother parsing the json.
                        # Depending on the where the tweet ends up being in the file, this optimization can speed up lookups by
                        # up to ~5x.
                        pass


class Tweets2013IaDocs(BaseDocs):
    def __init__(self, docs_dlcs, base_path):
        self._docs_dlcs = docs_dlcs
        self._docs_base_path = base_path
        self._docs_file_counts_cache = None

    def _id2file(self, snowflake_id):
        # Converts a tweet ID to a timestamp-based file path
        ts = ((snowflake_id >> 22) + 1288834974657) # "magic" numbers from https://github.com/client9/snowflake2time/blob/master/python/snowflake.py#L24
        dt = datetime.fromtimestamp(ts // 1000)
        return f'{dt.month:02d}/{dt.day:02d}/{dt.hour:02d}/{dt.minute:02d}.jsonl.lz4'

    def _docs_build(self):
        success_file = Path(self._docs_base_path) / '_success'
        inprogress_file = Path(self._docs_base_path) / '_inprogress'
        if success_file.exists():
            return # already built

        # Make sure there's not already another process building this structure. Having multiple processes work on
        # this concurrently would cause problems because we gotta append to files.
        inprogress_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            inprogress_file.touch(exist_ok=False)
        except FileExistsError:
            raise RuntimeError('Another process is currently building tweets2013-ia corpus; please wait for this process to finish. '
                               f'(If a prior process failed, you may need to manually clear {self._docs_base_path})')

        file_counts = Counter() # keeps track of the number of tweets in each file (for fancy slicing)

        try:
            # TODO: There's the potential for a race condition here...
            shutil.rmtree(inprogress_file.parent) # clear out directory because this process needs to *append* to files
            inprogress_file.parent.mkdir(parents=True, exist_ok=True)
            inprogress_file.touch(exist_ok=False)

            writer = TweetWriter(self._docs_base_path)
            with _logger.pbar_raw(desc='tweets') as pbar, contextlib.ExitStack() as stack:
                # Since the souce files download slowly anyway, download them in parallel (this doesn't seem to reduce the download speed of either file)
                dlc_streams = [stack.enter_context(dlc.stream()) for dlc in self._docs_dlcs]
                tar_files = [stack.enter_context(tarfile.open(fileobj=stream, mode='r|')) for stream in dlc_streams]

                # Loop through the tar sources file-by-file (alternating between them)
                for records in itertools.zip_longest(*tar_files):
                    for record, tarf in zip(records, tar_files):
                        if record is None or not record.name.endswith('.json.bz2'):
                            continue # not a data file

                        # Loop through the tweets in each file
                        with bz2.open(tarf.extractfile(record)) as f:
                            for line in f:
                                data = json.loads(line)
                                if 'id' not in data:
                                    continue # e.g., "delete" records
                                out_file = self._id2file(data['id'])
                                writer.add(out_file, data['id'], line)
                                file_counts[out_file] += 1
                                pbar.update(1)

            writer.flush() # any remaining tweets

            # Write out a file that gives the counts for each source file. This is used for fancy slicing
            # and also avoids globbing to get a list of all source files.
            with (Path(self._docs_base_path) / 'file_counts.tsv').open('wt') as f:
                for file, count in sorted(file_counts.items()):
                    f.write(f'{file}\t{count}\n')

            # Mark as done!
            success_file.touch()
        finally:
            # No longer working on it
            inprogress_file.unlink()

    def docs_iter(self):
        return Tweets2013IaDocIter(self, slice(0, self.docs_count()))

    def docs_cls(self):
        return TweetDoc

    def docs_store(self):
        return ir_datasets.indices.CacheDocstore(TweetsDocstore(self), f'{self.docs_path()}.cache')

    def docs_path(self):
        return self._docs_base_path

    def docs_count(self):
        return sum(self._docs_file_counts().values())

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return None # multiple languages

    def _docs_file_counts(self):
        if self._docs_file_counts_cache is None:
            self._docs_build()
            result = {}
            with (Path(self.docs_path()) / 'file_counts.tsv').open('rt') as f:
                for line in f:
                    file, count = line.strip().split('\t')
                    result[file] = int(count)
            self._docs_file_counts_cache = result
        return self._docs_file_counts_cache

    def _docs_iter_source_files(self):
        yield from self._docs_file_counts().keys()

    def _docs_ctxt_iter_tweets(self, source_file):
        lz4 = ir_datasets.lazy_libs.lz4_frame()
        with lz4.frame.LZ4FrameFile(Path(self._docs_base_path) / source_file) as fin:
            for line in fin:
                data = json.loads(line)
                if 'id' in data:
                    yield self._docs_source_to_doc(line, data)

    def _docs_source_to_doc(self, source, data):
        retweet_id = data['retweeted_status']['id_str'] if 'retweeted_status' in data else None
        return TweetDoc(data['id_str'], data['text'], data['user']['id_str'], data['created_at'], data.get('lang'), data['in_reply_to_status_id_str'], retweet_id, source, 'application/json')


def _init():
    subsets = {}
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    collection = Tweets2013IaDocs([dlc['docs/feb'], dlc['docs/mar']], os.path.join(base_path, 'corpus'))

    base = Dataset(collection, documentation('_'))

    subsets['trec-mb-2013'] = Dataset(
        collection,
        TrecQueries(dlc['trec-mb-2013/queries'], qtype=TrecMb13Query, qtype_map=QTYPE_MAP_13, remove_tags=RM_TAGS, namespace=NAME, lang='en'),
        TrecQrels(dlc['trec-mb-2013/qrels'], QREL_DEFS),
        documentation('trec-mb-2013')
    )

    subsets['trec-mb-2014'] = Dataset(
        collection,
        TrecQueries(dlc['trec-mb-2014/queries'], qtype=TrecMb14Query, qtype_map=QTYPE_MAP_14, remove_tags=RM_TAGS, namespace=NAME, lang='en'),
        TrecQrels(dlc['trec-mb-2014/qrels'], QREL_DEFS),
        documentation('trec-mb-2014')
    )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
