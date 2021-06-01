import re
import unittest
from ir_datasets.datasets.dpr_w100 import DprW100Doc, DprW100Query
from ir_datasets.formats import TrecQrel, TrecQuery
from .base import DatasetIntegrationTest


class TestDprW100(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('dpr-w100', count=21015324, items={
            0: DprW100Doc('1', re.compile('^"Aaron Aaron \\( or ; ""Ahärôn""\\) is a prophet, high priest, and the brother of Moses in the Abrahamic.{412} brother\'s spokesman \\(""prophet""\\) to the Pharaoh\\. Part of the Law \\(Torah\\) that Moses received from"$', flags=48), 'Aaron'),
            9: DprW100Doc('10', re.compile('^"families some time in Israel\'s past\\. Others argue that the story simply shows what can happen if th.{397}ho affirmed Moses\' uniqueness as the one with whom the spoke face to face\\. Miriam was punished with"$', flags=48), 'Aaron'),
            21015323: DprW100Doc('21015324', re.compile('^"committee was established before the building was opened\\. It is the District Nursing base for North.{425}ontains 81 extra care apartments two GP surgeries, a public library, a community café, an optician,"$', flags=48), '"Limelight centre"'),
        })

    def test_queries(self):
        self._test_queries('dpr-w100/natural-questions/train', count=58880, items={
            0: DprW100Query('0', 'big little lies season 2 how many episodes', ('seven',)),
            9: DprW100Query('9', 'who is in charge of enforcing the pendleton act of 1883', ('United States Civil Service Commission',)),
            58879: DprW100Query('58879', 'who plays the army guy in pitch perfect 3', ('Matt Lanter', 'Troy Ian Hall')),
        })
        self._test_queries('dpr-w100/natural-questions/dev', count=6515, items={
            0: DprW100Query('0', 'who sings does he love me with reba', ('Linda Davis',)),
            9: DprW100Query('9', 'what is the name of wonder womans mother', ('Queen Hippolyta',)),
            6514: DprW100Query('6514', 'girl from the shut up and dance video', ('Lauren Taft',)),
        })
        self._test_queries('dpr-w100/trivia-qa/train', count=78785, items={
            0: DprW100Query('0', 'Who was President when the first Peanuts cartoon was published?', ('Presidency of Harry S. Truman', 'Hary truman', 'Harry Shipp Truman', "Harry Truman's", 'Harry S. Truman', 'Harry S.Truman', 'Harry S Truman', 'H. S. Truman', 'President Harry Truman', 'Truman administration', 'Presidency of Harry Truman', 'Mr. Citizen', 'HST (president)', 'H.S. Truman', 'Mary Jane Truman', 'Harry Shippe Truman', 'S truman', 'Harry Truman', 'President Truman', '33rd President of the United States', 'Truman Administration', 'Harry Solomon Truman', 'Harold Truman', 'Harry truman', 'H. Truman')),
            9: DprW100Query('9', 'Which was the first European country to abolish capital punishment?', ('Norvège', 'Mainland Norway', 'Norway', 'Norvege', 'Noregur', 'NORWAY', 'Norwegian state', 'Etymology of Norway', 'Noruega', 'Norwegen', 'ISO 3166-1:NO', 'Noreg', 'Republic of Norway', 'Norwegian kingdom', 'Kongeriket Noreg', 'Name of Norway', 'Kongeriket Norge', 'Noorwegen', 'Kingdom of Norway', 'Sport in Norway', 'Norwegia', 'Royal Kingdom of Norway')),
            78784: DprW100Query('78784', 'According to the Bart Simpsons TV ad, Nobody better lay a finger on my what??', ('Butterfingers Snackerz', 'Butterfinger (ice cream)', 'Butterfinger Crisp', 'Nestlé Butterfinger', 'Butterfinger Snackerz', 'Butterfinger Ice Cream Bars', "Butterfinger BB's", 'Butterfinger', 'The Butterfinger Group')),
        })
        self._test_queries('dpr-w100/trivia-qa/dev', count=8837, items={
            0: DprW100Query('0', 'The VS-300 was a type of what?', ('🚁', 'Helicopters', 'Civilian helicopter', 'Pescara (helicopter)', 'Cargo helicopter', 'Copter', 'Helecopter', 'List of deadliest helicopter crashes', 'Helichopper', 'Helocopter', 'Cargo Helicopter', 'Helicopter', 'Helicoptor', 'Anatomy of a helicopter')),
            9: DprW100Query('9', 'Who wrote The Turn Of The Screw in the 19th century and The Ambassadors in the 20th?', ('The Finer Grain', 'Henry james', 'James, Henry', 'Henry James')),
            8836: DprW100Query('8836', 'Name the artist and the title of this 1978 classic that remains popular today: We were at the beach Everybody had matching towels Somebody went under a dock And there they saw a rock It wasnt a rock', ('Rock Lobster by the B-52s',)),
        })

    def test_qrels(self):
        self._test_qrels('dpr-w100/natural-questions/train', count=8856662, items={
            0: TrecQrel('0', '18768923', 2, '0'),
            9: TrecQrel('0', '928112', 0, '0'),
            8856661: TrecQrel('58879', '14546521', -1, '0'),
        })
        self._test_qrels('dpr-w100/natural-questions/dev', count=979893, items={
            0: TrecQrel('0', '11828866', 2, '0'),
            9: TrecQrel('0', '9446572', 0, '0'),
            979892: TrecQrel('6514', '11133390', -1, '0'),
        })
        self._test_qrels('dpr-w100/trivia-qa/train', count=7878500, items={
            0: TrecQrel('0', '525858', 0, '0'),
            9: TrecQrel('0', '16254256', 0, '0'),
            7878499: TrecQrel('78784', '5674041', 0, '0'),
        })
        self._test_qrels('dpr-w100/trivia-qa/dev', count=883700, items={
            0: TrecQrel('0', '7108855', 1, '0'),
            9: TrecQrel('0', '10764863', 0, '0'),
            883699: TrecQrel('8836', '9491145', 0, '0'),
        })

if __name__ == '__main__':
    unittest.main()
