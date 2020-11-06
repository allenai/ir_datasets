import io
import codecs
import re
import ir_datasets
from ir_datasets.util import Cache, TarExtract, IterStream, GzipExtract, Lazy, DownloadConfig
from ir_datasets.datasets.base import Dataset, FilteredQueries, FilteredScoredDocs, FilteredQrels, FilteredDocPairs, YamlDocumentation
from ir_datasets.formats import TsvQueries, TsvDocs, TrecQrels, TrecScoredDocs, TsvDocPairs

_logger = ir_datasets.log.easy()

DUA = ("Please confirm you agree to the MSMARCO data usage agreement found at "
       "<http://www.msmarco.org/dataset.aspx>")

QRELS_DEFS = {
    1: 'Labeled by crowd worker as relevant'
}

TREC_DL_QRELS_DEFS = {
    3: "Perfectly relevant: The passage is dedicated to the query and contains the exact answer.",
    2: "Highly relevant: The passage has some answer for the query, but the answer may be a bit "
       "unclear, or hiddenamongst extraneous information.",
    1: "Related: The passage seems related to the query but does not answer it.",
    0: "Irrelevant: The passage has nothing to do with the query.",
}

SPLIT200_QIDS = {'484694', '836399', '683975', '428803', '1035062', '723895', '267447', '325379', '582244', '148817', '44209', '1180950', '424238', '683835', '701002', '1076878', '289809', '161771', '807419', '530982', '600298', '33974', '673484', '1039805', '610697', '465983', '171424', '1143723', '811440', '230149', '23861', '96621', '266814', '48946', '906755', '1142254', '813639', '302427', '1183962', '889417', '252956', '245327', '822507', '627304', '835624', '1147010', '818560', '1054229', '598875', '725206', '811871', '454136', '47069', '390042', '982640', '1174500', '816213', '1011280', '368335', '674542', '839790', '270629', '777692', '906062', '543764', '829102', '417947', '318166', '84031', '45682', '1160562', '626816', '181315', '451331', '337653', '156190', '365221', '117722', '908661', '611484', '144656', '728947', '350999', '812153', '149680', '648435', '274580', '867810', '101999', '890661', '17316', '763438', '685333', '210018', '600923', '1143316', '445800', '951737', '1155651', '304696', '958626', '1043094', '798480', '548097', '828870', '241538', '337392', '594253', '1047678', '237264', '538851', '126690', '979598', '707766', '1160366', '123055', '499590', '866943', '18892', '93927', '456604', '560884', '370753', '424562', '912736', '155244', '797512', '584995', '540814', '200926', '286184', '905213', '380420', '81305', '749773', '850038', '942745', '68689', '823104', '723061', '107110', '951412', '1157093', '218549', '929871', '728549', '30937', '910837', '622378', '1150980', '806991', '247142', '55840', '37575', '99395', '231236', '409162', '629357', '1158250', '686443', '1017755', '1024864', '1185054', '1170117', '267344', '971695', '503706', '981588', '709783', '147180', '309550', '315643', '836817', '14509', '56157', '490796', '743569', '695967', '1169364', '113187', '293255', '859268', '782494', '381815', '865665', '791137', '105299', '737381', '479590', '1162915', '655989', '292309', '948017', '1183237', '542489', '933450', '782052', '45084', '377501', '708154'}


# Converts "top1000" MS run files "QID DID QText DText" to "QID DID" to remove tons of redundant
# storage with query and document files.
class ExtractQidPid:
    def __init__(self, streamer):
        self._streamer = streamer

    def stream(self):
        return io.BufferedReader(IterStream(iter(self)), buffer_size=io.DEFAULT_BUFFER_SIZE)

    def __iter__(self):
        with self._streamer.stream() as stream:
            for line in _logger.pbar(stream, desc='extracting QID/PID pairs'):
                qid, did, _, _ = line.split(b'\t')
                yield qid + b'\t' + did + b'\n'


# The encoding of the MS MARCO passage collection is... weird...
# Some characters are properly utf8-encoded, while others are not, even within the same passage.
# So, thi cutom-built streaming class aims to fix that. What it does is finds "suspicious"
# characters, basically anything in the 128-255 range. Once found, it will pick 2-3 characters
# around it and try to encode them as latin-1 and decode them at utf8.
# This seems to work well enough. Or at least better than anything else I've tried!
# There may be a more efficient way of doing this, but since it's done before caching the file,
# I'm not super concenred about the performance impact.
class FixEncoding:
    def __init__(self, streamer):
        self._streamer = streamer

    def stream(self):
        return io.BufferedReader(IterStream(iter(self)), buffer_size=io.DEFAULT_BUFFER_SIZE)

    def __iter__(self):
        SUS = '[\x80-\xff]'
        regex2 = re.compile(f'(.{SUS}|{SUS}.)')
        regex3 = re.compile(f'(..{SUS}|.{SUS}.|{SUS}..)')

        with self._streamer.stream() as stream, \
             _logger.pbar_raw(desc='fixing encoding', unit='B', unit_scale=True) as pbar:
            stream = codecs.getreader('utf8')(stream)
            for line in stream:
                pbar.update(len(line.encode()))
                pos = 0
                while pos < len(line):
                    match = regex3.search(line, pos=pos)
                    if not match:
                        break
                    try:
                        fixed = match.group().encode('latin1').decode('utf8')
                        if len(fixed) == 1:
                            line = line[:match.start()] + fixed + line[match.end():]
                    except UnicodeError:
                        pass
                    pos = match.start() + 1
                pos = 0
                while pos < len(line):
                    match = regex2.search(line, pos=pos)
                    if not match:
                        break
                    try:
                        fixed = match.group().encode('latin1').decode('utf8')
                        if len(fixed) == 1:
                            line = line[:match.start()] + fixed + line[match.end():]
                    except UnicodeError:
                        pass
                    pos = match.start() + 1
                yield line.encode()


def _init():
    documentation = YamlDocumentation('docs/msmarco-passage.yaml')
    base_path = ir_datasets.util.cache_path()/'msmarco-passage'
    dlc = DownloadConfig.context('msmarco-passage', base_path, dua=DUA)
    collection = TsvDocs(Cache(FixEncoding(TarExtract(dlc['docs'], 'collection.tsv')), base_path/'collection.tsv'))
    subsets = {}

    subsets['train'] = Dataset(
        collection,
        TsvQueries(Cache(TarExtract(dlc['queries'], 'queries.train.tsv'), base_path/'train/queries.tsv')),
        TrecQrels(dlc['train/qrels'], QRELS_DEFS),
        TsvDocPairs(GzipExtract(dlc['train/docpairs'])),
        TrecScoredDocs(Cache(ExtractQidPid(TarExtract(dlc['train/scoreddocs'], 'top1000.train.txt')), base_path/'train/ms.run')),
    )

    subsets['dev'] = Dataset(
        collection,
        TsvQueries(Cache(TarExtract(dlc['queries'], 'queries.dev.tsv'), base_path/'dev/queries.tsv')),
        TrecQrels(dlc['dev/qrels'], QRELS_DEFS),
        TrecScoredDocs(Cache(ExtractQidPid(TarExtract(dlc['dev/scoreddocs'], 'top1000.dev')), base_path/'dev/ms.run')),
    )

    subsets['eval'] = Dataset(
        collection,
        TsvQueries(Cache(TarExtract(dlc['queries'], 'queries.eval.tsv'), base_path/'eval/queries.tsv')),
        TrecScoredDocs(Cache(ExtractQidPid(TarExtract(dlc['eval/scoreddocs'], 'top1000.eval')), base_path/'eval/ms.run')),
    )

    subsets['trec-dl-2019'] = Dataset(
        collection,
        TrecQrels(dlc['trec-dl-2019/qrels'], TREC_DL_QRELS_DEFS),
        TsvQueries(Cache(GzipExtract(dlc['trec-dl-2019/queries']), base_path/'trec-dl-2019/queries.tsv')),
        TrecScoredDocs(Cache(ExtractQidPid(GzipExtract(dlc['trec-dl-2019/scoreddocs'])), base_path/'trec-dl-2019/ms.run')),
    )

    subsets['trec-dl-2020'] = Dataset(
        collection,
        TsvQueries(GzipExtract(dlc['trec-dl-2020/queries'])),
        TrecScoredDocs(Cache(ExtractQidPid(GzipExtract(dlc['trec-dl-2020/scoreddocs'])), base_path/'trec-dl-2020/ms.run')),
    )

    # A few subsets that are contrainted to just the queries/qrels/docpairs that have at least
    # 1 relevance assessment
    train_judged = Lazy(lambda: {q.query_id for q in subsets['train'].qrels_iter()})
    subsets['train/judged'] = Dataset(
        FilteredQueries(subsets['train'].queries_handler(), train_judged),
        FilteredScoredDocs(subsets['train'].scoreddocs_handler(), train_judged),
        subsets['train'],
    )

    dev_judged = Lazy(lambda: {q.query_id for q in subsets['dev'].qrels_iter()})
    subsets['dev/judged'] = Dataset(
        FilteredQueries(subsets['dev'].queries_handler(), dev_judged),
        FilteredScoredDocs(subsets['dev'].scoreddocs_handler(), dev_judged),
        subsets['dev'],
    )

    dl19_judged = Lazy(lambda: {q.query_id for q in subsets['trec-dl-2019'].qrels_iter()})
    subsets['trec-dl-2019/judged'] = Dataset(
        FilteredQueries(subsets['trec-dl-2019'].queries_handler(), dl19_judged),
        FilteredScoredDocs(subsets['trec-dl-2019'].scoreddocs_handler(), dl19_judged),
        subsets['trec-dl-2019'],
    )

    # split200 -- 200 queries held out from the training data for validation
    split200 = Lazy(lambda: SPLIT200_QIDS)
    subsets['train/split200-train'] = Dataset(
        FilteredQueries(subsets['train'].queries_handler(), split200, mode='exclude'),
        FilteredScoredDocs(subsets['train'].scoreddocs_handler(), split200, mode='exclude'),
        FilteredQrels(subsets['train'].qrels_handler(), split200, mode='exclude'),
        FilteredDocPairs(subsets['train'].docpairs_handler(), split200, mode='exclude'),
        subsets['train'],
    )
    subsets['train/split200-valid'] = Dataset(
        FilteredQueries(subsets['train'].queries_handler(), split200, mode='include'),
        FilteredScoredDocs(subsets['train'].scoreddocs_handler(), split200, mode='include'),
        FilteredQrels(subsets['train'].qrels_handler(), split200, mode='include'),
        FilteredDocPairs(subsets['train'].docpairs_handler(), split200, mode='include'),
        subsets['train'],
    )

    # Medical subset
    def train_med():
        with dlc['medmarco_ids'].stream() as stream:
            stream = codecs.getreader('utf8')(stream)
            return {l.rstrip() for l in stream}
    train_med = Lazy(train_med)
    subsets['train/medical'] = Dataset(
        FilteredQueries(subsets['train'].queries_handler(), train_med),
        FilteredScoredDocs(subsets['train'].scoreddocs_handler(), train_med),
        FilteredDocPairs(subsets['train'].docpairs_handler(), train_med),
        FilteredQrels(subsets['train'].qrels_handler(), train_med),
        subsets['train'],
    )

    ir_datasets.registry.register('msmarco-passage', Dataset(collection, documentation('_')))
    for s in sorted(subsets):
        ir_datasets.registry.register(f'msmarco-passage/{s}', Dataset(subsets[s], documentation(s)))

    return collection, subsets


collection, subsets = _init()
