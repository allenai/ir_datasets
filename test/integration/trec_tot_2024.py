import re
import unittest
from ir_datasets.formats import TrecQrel
from ir_datasets.datasets.trec_tot import TipOfTheTongueDoc2024, TipOfTheTongueQuery2024
from test.integration.base import DatasetIntegrationTest
import ir_datasets


class TestTipOfTheTongue(DatasetIntegrationTest):
    def test_tip_of_the_tongue_docs(self):
        self._test_docs('trec-tot/2024', count=3185450, items={
            0: TipOfTheTongueDoc2024("846", "Museum of Work", "Q6941060", re.compile("^The Museum of Work .*"), [{"start": 0, "end": 798, "section": "Abstract"}, {"start": 798, "end": 1620, "section": "Overview"}, {"start": 1620, "end": 3095, "section": "Exhibitions"}, {"start": 3095, "end": 3371, "section": "The history of Alva"}, {"start": 3371, "end": 3824, "section": "Industriland"}, {"start": 3824, "end": 4371, "section": "Framtidsland (Future country)"}, {"start": 4371, "end": 4761, "section": "EWK \u2014 The Center for Political Illustration Art"}]),
            1091: TipOfTheTongueDoc2024("9764", "Emma Goldman", "Q79969", re.compile("Emma Goldman \\(June 27, 1869 .*"),[{"start": 0, "end": 2752, "section": "Abstract"}, {"start": 2752, "end": 45613, "section": "Biography"}, {"start": 45613, "end": 47371, "section": "Family"}, {"start": 47371, "end": 50317, "section": "Adolescence"}, {"start": 50317, "end": 52433, "section": "Rochester, New York"}, {"start": 52433, "end": 54427, "section": "Most and Berkman"}, {"start": 54427, "end": 57448, "section": "Homestead plot"}, {"start": 57448, "end": 60672, "section": "\"Inciting to riot\""}, {"start": 60672, "end": 63288, "section": "McKinley assassination"}, {"start": 63288, "end": 66975, "section": "''Mother Earth'' and Berkman's release"}, {"start": 66975, "end": 69914, "section": "Reitman, essays, and birth control"}, {"start": 69914, "end": 73788, "section": "World War I"}, {"start": 73788, "end": 76344, "section": "Deportation"}, {"start": 76344, "end": 79375, "section": "Russia"}, {"start": 79375, "end": 83782, "section": "England, Canada, and France"}, {"start": 83782, "end": 86917, "section": "Spanish Civil War"}, {"start": 86917, "end": 87430, "section": "Final years"}, {"start": 87430, "end": 88493, "section": "Death"}, {"start": 88493, "end": 101764, "section": "Philosophy"}, {"start": 101764, "end": 106976, "section": "Anarchism"}, {"start": 106976, "end": 109922, "section": "Tactical uses of violence"}, {"start": 109922, "end": 111036, "section": "Capitalism and labor"}, {"start": 111036, "end": 114245, "section": "State"}, {"start": 114245, "end": 116281, "section": "Feminism and sexuality"}, {"start": 116281, "end": 117248, "section": "Atheism"}, {"start": 117248, "end": 120736, "section": "Legacy"}, {"start": 120736, "end": 120977, "section": "Works"}])
        })

    def test_tip_of_the_tongue_queries(self):
        self._test_queries('trec-tot/2024/test', count=600, items={
            0: TipOfTheTongueQuery2024("2001", re.compile("^I remember this old building I used to pass by in the heart of a bustling financial district, a place where the air always seemed thick.*")),
            599: TipOfTheTongueQuery2024("2600", re.compile("^Okay, this is a vague one .\n So I know this is going to be.*"))
        })

if __name__ == '__main__':
    unittest.main()

