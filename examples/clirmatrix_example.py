import ir_datasets
"""
dataset name
clirmatrix/[query language code]/dataset/[doc language code]/[split]

options:
--------
    dataset: bi139-base/bi139-full/multi8
    supported query/doc language codes:
        bi139-base/bi139-full: ['af', 'als', 'am', 'an', 'ar', 'arz', 'ast', 'az', 'azb', 'ba', 'bar', 'be', 'bg', 'bn', 'bpy', 'br', 'bs', 'bug', 'ca', 'cdo', 'ce', 'ceb', 'ckb', 'cs', 'cv', 'cy', 'da', 'de', 'diq', 'el', 'eml', 'en', 'eo', 'es', 'et', 'eu', 'fa', 'fi', 'fo', 'fr', 'fy', 'ga', 'gd', 'gl', 'gu', 'he', 'hi', 'hr', 'hsb', 'ht', 'hu', 'hy', 'ia', 'id', 'ilo', 'io', 'is', 'it', 'ja', 'jv', 'ka', 'kk', 'kn', 'ko', 'ku', 'ky', 'la', 'lb', 'li', 'lmo', 'lt', 'lv', 'mai', 'mg', 'mhr', 'min', 'mk', 'ml', 'mn', 'mr', 'mrj', 'ms', 'my', 'mzn', 'nap', 'nds', 'ne', 'new', 'nl', 'nn', 'no', 'oc', 'or', 'os', 'pa', 'pl', 'pms', 'pnb', 'ps', 'pt', 'qu', 'ro', 'ru', 'sa', 'sah', 'scn', 'sco', 'sd', 'sh', 'si', 'simple', 'sk', 'sl', 'sq', 'sr', 'su', 'sv', 'sw', 'szl', 'ta', 'te', 'tg', 'th', 'tl', 'tr', 'tt', 'uk', 'ur', 'uz', 'vec', 'vi', 'vo', 'wa', 'war', 'wuu', 'xmf', 'yi', 'yo', 'zh']
        multi8: ['ar', 'de', 'en', 'es', 'fr', 'ja', 'ru', 'zh']

    split: train/dev/test1/test2
"""

#examples
#reference python notebook: https://colab.research.google.com/github/allenai/ir_datasets/blob/master/examples/ir_datasets.ipynb#scrollTo=n7mY16MRH0hx
dataset = ir_datasets.load("clirmatrix/en/bi139-base/zh/test1")
docstore = dataset.docs_store()

for qrels in dataset.qrels_iter():
    print(docstore.get(qrels.doc_id))
    break

for query in dataset.queries_iter():
    print(query)
    break


dataset = ir_datasets.load("clirmatrix/en/multi8/zh/train")
docstore = dataset.docs_store()

for qrels in dataset.qrels_iter():
    print(docstore.get(qrels.doc_id))
    break

for query in dataset.queries_iter():
    print(query)
    break


dataset = ir_datasets.load("clirmatrix/an/bi139-full/zh/dev")
docstore = dataset.docs_store()

for qrels in dataset.qrels_iter():
    print(docstore.get(qrels.doc_id))
    break

for query in dataset.queries_iter():
    print(query)
    break
