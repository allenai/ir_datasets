import re
import unittest
from ir_datasets.formats import TrecQrel
from ir_datasets.datasets.trec_fair_2021 import FairTrecDoc, FairTrecQuery
from base import DatasetIntegrationTest


class TestFairTrec(DatasetIntegrationTest):
    def test_docs(self):
        self._build_test_docs("trec_fair_2021",include_count=False)
        

    def test_queries(self):
        self._build_test_queries("trec_fair_2021")
        

    def test_qrels(self):
        self._build_test_qrels("trec_fair_2021")
       
"""trec_fair_2021 docs: 1000it [00:00, 8012.38it/s]
[INFO] 
self._test_docs('trec_fair_2021', items={
    0: FairTrecDoc(12, 'Anarchism', re.compile('^\\{\\{short description\\|Political philosophy and movement\\}\\}\n\\{\\{redirect2\\|Anarchist\\|Anarchists\\|other uses\\|.{96002}al movements\\]\\]\n\\[\\[Category:Political ideologies\\]\\]\n\\[\\[Category:Social theories\\]\\]\n\\[\\[Category:Socialism\\]\\]$', flags=48), re.compile('^\n\n\n\n\n\n\n\n\nAnarchism is a political philosophy and movement that is sceptical of authority and rejects.{46355}ance without government\n\n\nList of anarchist political ideologies\n\n\nList of books about anarchism\n\n\n\n$', flags=48), 'https://en.wikipedia.org/wiki/Anarchism', '0.909', [], 'FA'),
    9: FairTrecDoc(316, 'Academy Award for Best Production Design', re.compile('^\\{\\{Use mdy dates\\|date=June 2013\\}\\}\n\\{\\{Infobox award\n\\| name      = Academy Award for Best Production Des.{97494}\\]\n\\[\\[Category:Best Art Direction Academy Award winners\\|\\*\\]\\]\n\\[\\[Category:Awards for best art direction\\]\\]$', flags=48), re.compile("^\n\n\nThe Academy Award for Best Production Design recognizes achievement for art direction in film\\. Th.{829} Award for Best Production Design\n\n\nCritics' Choice Movie Award for Best Art Direction\n\n\n\nNotes\n\n\n\n\n$", flags=48), 'https://en.wikipedia.org/wiki/Academy_Award_for_Best_Production_Design', '0.67862654', ['Northern America'], 'GA'),
    1000: FairTrecDoc(2459, 'Appomattox', re.compile("^'''Appomattox''', shorthand for the surrender of Robert E\\. Lee to Ulysses S\\. Grant in the American C.{1338}dria, Virginia\n\n== See also ==\n\\*\\{\\{in title\\|Appomattox\\}\\}\n\\*\\[\\[Appomattoc\\]\\] \\(people\\)\n\n\\{\\{disambiguation\\}\\}$", flags=48), re.compile('^Appomattox, shorthand for the surrender of Robert E\\. Lee to Ulysses S\\. Grant in the American Civil W.{1139}a bronze Confederate soldier memorial in Alexandria, Virginia\n\n\n\nSee also\n\n\n\nAppomattoc \\(people\\)\n\n\n\n$', flags=48), 'https://en.wikipedia.org/wiki/Appomattox', '0.07619508', [], 'Stub'),
})

trec_fair_2021 qrels: 2185446it [00:02, 984830.27it/s]
[INFO] [finished] trec_fair_2021 qrels: [00:02] [2185446it] [984776.94it/s]
[INFO] 
self._test_qrels('trec_fair_2021', count=2185446, items={
    0: TrecQrel(1, 572, 1, 0),
    9: TrecQrel(1, 4514, 1, 0),
    2185445: TrecQrel(57, 67253426, 1, 0),
})

trec_fair_2021 queries: 57it [00:00, 182.83it/s]
[INFO] [finished] trec_fair_2021 queries: [00:00] [57it] [182.75it/s]
[INFO] 
self._test_queries('trec_fair_2021', count=57, items={
    0: FairTrecQuery(1, 'Agriculture'),
    9: FairTrecQuery(10, 'Buddhism'),
    56: FairTrecQuery(57, 'Nigeria'),
})
"""
if __name__ == '__main__':
    unittest.main()
