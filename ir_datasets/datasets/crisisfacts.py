import json
import contextlib
import requests
import io
import codecs
import itertools
import ir_datasets
from typing import NamedTuple, Dict, Any
from ir_datasets import util
from ir_datasets.util import DownloadConfig, TarExtract, Cache
from ir_datasets.formats import BaseDocs, BaseQueries, BaseQrels
from ir_datasets.formats import GenericDoc, GenericQuery, TrecQrel
from ir_datasets.indices import PickleLz4FullStore
from ir_datasets.datasets.base import Dataset, YamlDocumentation


_logger = ir_datasets.log.easy()

NAME = 'crisisfacts'

base_url = "http://demos.terrier.org/crisisfacts"


class CrisisFactsApiDownload:
    def __init__(self, url_path, eventNumber, requestDate, stream=True):
        self.url_path = url_path
        self.eventNumber = eventNumber
        self.requestDate = requestDate
        self._stream = stream

    def register(self):
        API_ENDPOINT = base_url+"/register"

        data = {
            'eventID': self.eventNumber,
            'requestDate': self.requestDate,
            **self._handle_auth(),
        }

        with _logger.duration('requesting access key'):
            r = requests.post(url = API_ENDPOINT, json=data)
            r.raise_for_status()

            result = r.json()

            if result['accessKey'] is None:
                raise RuntimeError("Error registering: {message}".format(**result))
            return result['accessKey']

    @contextlib.contextmanager
    def stream(self):
        with io.BufferedReader(util.IterStream(iter(self)), buffer_size=io.DEFAULT_BUFFER_SIZE) as stream:
            yield stream

    def __iter__(self):
        accessKey = self.register()
        endOfStream = False
        while (not endOfStream): # we are going to iteratively request data until there is none left
            if not self._stream:
                endOfStream = True
            API_ENDPOINT = base_url + self.url_path
            PARAMS = {'accessKey':accessKey}
            r = requests.get(url = API_ENDPOINT, params = PARAMS, timeout=60) # longer timeout than default
            r.raise_for_status()
            if r.content:
                yield r.content
                yield b'\n'
            else:
                endOfStream = True

    def _handle_auth(self):
        auth_dir = util.home_path() / 'auth'
        if not auth_dir.exists():
            auth_dir.mkdir(parents=True, exist_ok=True)
        auth_path = auth_dir / 'crisisfacts.json'
        if auth_path.exists():
            with auth_path.open('rt') as fin:
                return json.load(fin)
        else:
            _logger.info('To download crisisfacts data, you need to provide your institution, contact person, email, and institution type.\n\nTo avoid this message in the future, you may '
                         'also set them in a file named {auth_path}, in the format of {{"institution": "", "contactname": "", "email": "", "institutiontype": ""}}.'.format(auth_path=str(auth_path)))
            institution = input('institution: ')
            contactname = input('contactname: ')
            email = input('email: ')
            institutiontype = input('institutiontype: ')
            return {'institution': institution, 'contactname': contactname, 'email': email, 'institutiontype': institutiontype}



class JsonlDocs(BaseDocs):
    def __init__(self, name, dlc, doc_type, field_map):
        super().__init__()
        self._name = name
        self._dlc = dlc
        self._doc_type = doc_type
        self._field_map = field_map

    def docs_iter(self):
        return iter(self.docs_store())

    def _docs_iter_first(self):
        with self._dlc.stream() as stream:
            for line in stream:
                for doc in json.loads(line):
                    yield self._doc_type(**{dest: doc[src] for dest, src in self._field_map.items()})

    def docs_cls(self):
        return self._doc_type

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{ir_datasets.util.home_path()/NAME/self._name}/docs.pklz4',
            init_iter_fn=self._docs_iter_first,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
            count_hint=ir_datasets.util.count_hint(f'{NAME}/{self._name}'),
        )

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

    def docs_namespace(self):
        return NAME


class CrisisFactsQueries(BaseQueries):
    def __init__(self, dlc):
        super().__init__()
        self._dlc = dlc

    def queries_iter(self):
        with self._dlc.stream() as f:
            data = json.load(f)
            event = data['event']
            for query in data['queries']:
                yield CrisisFactsQuery(
                    query['queryID'],
                    query['query'],
                    query['indicativeTerms'],
                    query['trecisCategoryMapping'],
                    event["eventID"],
                    event["title"],
                    event["dataset"],
                    event["description"],
                    event["trecisID"],
                    event["type"],
                    event["url"],
                )

    def queries_cls(self):
        return CrisisFactsQuery

    def queries_count(self):
        return sum(1 for _ in self.queries_iter())

    def queries_namespace(self):
        return NAME

    def queries_lang(self):
        return 'en'


class CrisisFactsStreamDoc(NamedTuple):
    doc_id: str
    event: str
    text: str
    source: Dict[str, Any]
    source_type: str
    unix_timestamp: int
    def default_text(self):
        return self.text


class CrisisFactsQuery(NamedTuple):
    query_id: str
    text: str
    indicative_terms: str
    trecis_category_mapping: str
    event_id: str
    event_title: str
    event_dataset: str
    event_description: str
    event_trecis_id: str
    event_type: str
    event_url: str

def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    subsets = {}

    base = Dataset(
        documentation('_'),
    )

    subsets = {}
    event_date_map = {
        '001': ['2017-12-07', '2017-12-08', '2017-12-09', '2017-12-10', '2017-12-11', '2017-12-12', '2017-12-13', '2017-12-14', '2017-12-15'],
        '002': ['2018-07-25', '2018-07-26', '2018-07-27', '2018-07-28', '2018-07-29', '2018-07-30'],
        '003': ['2018-08-06', '2018-08-07', '2018-08-08', '2018-08-09', '2018-08-10', '2018-08-12', '2018-08-13'],
        '004': ['2018-09-01', '2018-09-04', '2018-09-05', '2018-09-07', '2018-09-08', '2018-09-09', '2018-09-10', '2018-09-11', '2018-09-12', '2018-09-13', '2018-09-14', '2018-09-15', '2018-09-16', '2018-09-17', '2018-09-18'],
        '005': ['2018-05-27', '2018-05-28', '2018-05-29', '2018-05-30'],
        '006': ['2019-10-10', '2019-10-11', '2019-10-12', '2019-10-13'],
        '007': ['2020-08-27', '2020-08-28'],
        '008': ['2020-09-11', '2020-09-12', '2020-09-13', '2020-09-14', '2020-09-15', '2020-09-16', '2020-09-17', '2020-09-18'],
        '009': ['2020-08-03', '2020-08-04', '2020-08-05', '2020-08-06', '2020-08-07', '2020-08-08', '2020-08-09'],
        '010': ['2020-01-23', '2020-01-24', '2020-01-25', '2020-01-26', '2020-01-27', '2020-01-28', '2020-01-29'],
        '011': ['2020-02-04', '2020-02-05', '2020-02-06', '2020-02-07', '2020-02-08'],
        '012': ['2020-05-02', '2020-05-03', '2020-05-04', '2020-05-05', '2020-05-06', '2020-05-07', '2020-05-08'],
        '013': ['2020-05-23', '2020-05-24', '2020-05-25', '2020-05-26', '2020-05-27', '2020-05-28', '2020-05-29'],
        '014': ['2019-08-29', '2019-08-30', '2019-08-31', '2019-09-01', '2019-09-02', '2019-09-03', '2019-09-04'],
        '015': ['2019-10-25', '2019-10-26', '2019-10-27', '2019-10-28', '2019-10-29', '2019-10-30', '2019-10-31'],
        '016': ['2020-04-12', '2020-04-13', '2020-04-14', '2020-04-15', '2020-04-16'],
        '017': ['2020-04-21', '2020-04-22', '2020-04-23', '2020-04-24', '2020-04-25', '2020-04-26'],
        '018': ['2020-03-02', '2020-03-03', '2020-03-04', '2020-03-05', '2020-03-06', '2020-03-07'],
    }
    for event, dates in event_date_map.items():
        for date in dates:
            subsets[f'{event}/{date}'] = Dataset(
                JsonlDocs(f'{event}/{date}', CrisisFactsApiDownload("/stream", event, date), CrisisFactsStreamDoc, {"doc_id": "streamID", "event": "event", "source_type": "sourceType", "source": "source", "text": "text", "unix_timestamp": "unixTimestamp"}),
                CrisisFactsQueries(Cache(CrisisFactsApiDownload("/queries", event, date, stream=False), base_path/date/event/'queries.json')),
            )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
