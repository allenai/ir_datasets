import io
import ir_datasets
from ir_datasets.formats import TsvDocs, TrecQrels, TsvQueries
from ir_datasets.util import DownloadConfig, Lazy
from .base import Dataset, FilteredQueries, FilteredQrels, YamlDocumentation

__all__ = ['collection', 'subsets']
_logger = ir_datasets.log.easy()


DUA = ("Please confirm you agree to the authors' data usage agreement found at "
       "<https://ciir.cs.umass.edu/downloads/Antique/readme.txt>")

# Qrel defs taken verbatim from <https://arxiv.org/pdf/1905.08957.pdf>
QREL_DEFS = {
    4: "It looks reasonable and convincing. Its quality is on parwith or better than the "
       "\"Possibly Correct Answer\". Note that it does not have to provide the same answer "
       "as the \"PossiblyCorrect Answer\".",
    3: "It can be an answer to the question, however, it is notsufficiently convincing. "
       "There should be an answer with much better quality for the question.",
    2: "It does not answer the question or if it does, it provides anunreasonable answer, "
       "however, it is not out of context. Therefore, you cannot accept it as an answer to "
       "the question.",
    1: "It is completely out of context or does not make any sense.",
}

VALIDATION_QIDS = {'1158088', '4032777', '1583099', '263783', '4237144', '1097878', '114758', '1211877', '1188438', '2689609', '1191621', '2571912', '1471877', '2961191', '2630860', '4092472', '3178012', '358253', '3913653', '844617', '2764765', '212427', '220575', '11706', '4069320', '3280274', '3159749', '4217473', '4042061', '1037897', '103298', '332662', '752633', '2704', '3635284', '2235825', '3651236', '2155390', '3752394', '2008456', '98438', '511835', '1647624', '3884772', '1536937', '544869', '66151', '2678635', '963523', '1881436', '993601', '3608433', '2048278', '3124162', '1907320', '1970273', '2891885', '2858043', '189364', '397709', '3470651', '3885753', '1933929', '94629', '2500918', '1708787', '2492366', '17665', '278043', '643630', '1727343', '196651', '3731489', '2910592', '1144768', '2573745', '546552', '1341602', '317469', '2735795', '1251077', '3507499', '3374970', '1034050', '1246269', '2901754', '2137263', '1295284', '2180502', '406082', '1443637', '2620488', '3118286', '3814583', '3738877', '684633', '2094435', '242701', '2613648', '2942624', '1495234', '1440810', '2421078', '961127', '595342', '363519', '4048305', '485408', '2573803', '3104841', '3626847', '727663', '3961', '4287367', '2112535', '913424', '1514356', '1512776', '937635', '1321784', '1582044', '1467322', '461995', '884643', '4338583', '2550445', '4165672', '1016750', '1184520', '3152714', '3617468', '3172166', '4031702', '2534994', '2035638', '404359', '1398838', '4183127', '2418824', '2439070', '2632334', '4262151', '3841762', '4400543', '2147417', '514804', '1423289', '2041828', '2776069', '1458676', '3407617', '1450678', '1978816', '2466898', '1607303', '2175167', '772988', '1289770', '3382182', '3690922', '1051346', '344029', '2357505', '1907847', '2587810', '3272207', '2522067', '1107012', '554539', '489705', '3652886', '4287894', '4387641', '1727879', '348777', '566364', '2678484', '4450252', '986260', '4336509', '3824106', '2169746', '2700836', '3495304', '3083719', '126182', '1607924', '1485589', '3211282', '2546730', '2897078', '3556937', '2113006', '929821', '2306533', '2543919', '1639607', '3958214', '2677193', '763189'}


def _init():
    documentation = YamlDocumentation('docs/antique.yaml')
    base_path = ir_datasets.util.cache_path() / 'antique'
    dlc = DownloadConfig.context('antique', base_path, dua=DUA)
    collection = TsvDocs(dlc['docs'])

    subsets = {}
    for subset in ('train', 'test'):
        qrels = TrecQrels(dlc[f'{subset}/qrels'], QREL_DEFS)
        queries = TsvQueries(dlc[f'{subset}/queries'])
        subsets[subset] = Dataset(collection, queries, qrels)

    # Split the training data into training and validation data
    validation_qids = Lazy(lambda: VALIDATION_QIDS)
    subsets['train/split200-train'] = Dataset(
        FilteredQueries(subsets['train'].queries_handler(), validation_qids, mode='exclude'),
        FilteredQrels(subsets['train'].qrels_handler(), validation_qids, mode='exclude'),
        subsets['train'])
    subsets['train/split200-valid'] = Dataset(
        FilteredQueries(subsets['train'].queries_handler(), validation_qids, mode='include'),
        FilteredQrels(subsets['train'].qrels_handler(), validation_qids, mode='include'),
        subsets['train'])

    # Separate test set removing the "offensive (and noisy)" questions
    disallow_list = dlc['disallow_list']
    def disllow_qids():
        with disallow_list.stream() as stream:
            stream = io.TextIOWrapper(stream)
            return {l.rstrip() for l in stream}
    disllow_qids = Lazy(disllow_qids)
    subsets['test/non-offensive'] = Dataset(
        FilteredQueries(subsets['test'].queries_handler(), disllow_qids, mode='exclude'),
        FilteredQrels(subsets['test'].qrels_handler(), disllow_qids, mode='exclude'),
        subsets['test'])

    ir_datasets.registry.register('antique', Dataset(collection, documentation('_')))

    for s in sorted(subsets):
        ir_datasets.registry.register(f'antique/{s}', Dataset(subsets[s], documentation(s)))

    return collection, subsets


collection, subsets = _init()
