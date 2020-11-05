import ir_datasets
from ir_datasets.util import GzipExtract, Lazy, DownloadConfig
from ir_datasets.formats import TrecQrels, TrecDocs, TrecQueries
from ir_datasets.datasets.base import Dataset, FilteredQueries, FilteredQrels, YamlDocumentation


NAME = 'trec-robust04'


QREL_DEFS = {
    2: 'highly relevant',
    1: 'relevant',
    0: 'not relevant',
}

DUA = ("Please confirm you agree to the TREC Robust 2004 data usage agreement found at "
       "<https://trec.nist.gov/data/cd45/index.html>")


# folds from <https://github.com/faneshion/DRMM/blob/9d348640ef8a56a8c1f2fa0754fe87d8bb5785bd/NN4IR.cpp>
FOLDS = {
    'fold1': {'302', '303', '309', '316', '317', '319', '323', '331', '336', '341', '356', '357', '370', '373', '378', '381', '383', '392', '394', '406', '410', '411', '414', '426', '428', '433', '447', '448', '601', '607', '608', '612', '617', '619', '635', '641', '642', '646', '647', '654', '656', '662', '665', '669', '670', '679', '684', '690', '692', '700'},
    'fold2': {'301', '308', '312', '322', '327', '328', '338', '343', '348', '349', '352', '360', '364', '365', '369', '371', '374', '386', '390', '397', '403', '419', '422', '423', '424', '432', '434', '440', '446', '602', '604', '611', '623', '624', '627', '632', '638', '643', '651', '652', '663', '674', '675', '678', '680', '683', '688', '689', '695', '698'},
    'fold3': {'306', '307', '313', '321', '324', '326', '334', '347', '351', '354', '358', '361', '362', '363', '376', '380', '382', '396', '404', '413', '415', '417', '427', '436', '437', '439', '444', '445', '449', '450', '603', '605', '606', '614', '620', '622', '626', '628', '631', '637', '644', '648', '661', '664', '666', '671', '677', '685', '687', '693'},
    'fold4': {'320', '325', '330', '332', '335', '337', '342', '344', '350', '355', '368', '377', '379', '387', '393', '398', '402', '405', '407', '408', '412', '420', '421', '425', '430', '431', '435', '438', '616', '618', '625', '630', '633', '636', '639', '649', '650', '653', '655', '657', '659', '667', '668', '672', '673', '676', '682', '686', '691', '697'},
    'fold5': {'304', '305', '310', '311', '314', '315', '318', '329', '333', '339', '340', '345', '346', '353', '359', '366', '367', '372', '375', '384', '385', '388', '389', '391', '395', '399', '400', '401', '409', '416', '418', '429', '441', '442', '443', '609', '610', '613', '615', '621', '629', '634', '640', '645', '658', '660', '681', '694', '696', '699'}
}


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.cache_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path, dua=DUA)
    subsets = {}

    collection = TrecDocs(dlc['docs'], path_globs=['**/FBIS/FB*.gz', '**/FR94/*/FR*.gz', '**/FT/*/FT*.gz', '**/LATIMES/LA*.gz'])

    queries = TrecQueries(GzipExtract(dlc['queries']))
    qrels = TrecQrels(dlc['qrels'], QREL_DEFS)

    base = Dataset(collection, queries, qrels, documentation('_'))

    for fold in FOLDS:
        qid_filter = make_filter(fold)
        subsets[fold] = Dataset(
            FilteredQueries(queries, qid_filter),
            FilteredQrels(qrels, qid_filter),
            collection,
            documentation(fold))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


def make_filter(fold):
    return Lazy(lambda: FOLDS[fold])


base, subsets = _init()
