import unittest
import ir_datasets
from ir_datasets.formats import GenericDoc, GenericQuery, TrecQrel
from .base import DatasetIntegrationTest

class TestAntique(DatasetIntegrationTest):
    def test_antique(self):
        self._test_docs('antique', count=403_666, items={
            0: GenericDoc(doc_id="2020338_0", text="A small group of politicians believed strongly that the fact that Saddam Hussien remained in power after the first Gulf War was a signal of weakness to the rest of the world, one that invited attacks and terrorism. Shortly after taking power with George Bush in 2000 and after the attack on 9/11, they were able to use the terrorist attacks to justify war with Iraq on this basis and exaggerated threats of the development of weapons of mass destruction. The military strength of the U.S. and the brutality of Saddam's regime led them to imagine that the military and political victory would be relatively easy."),
            9: GenericDoc(doc_id="1908421_1", text="The gas mileage for some hybrids is better in the city because they spend more time using their electric motor instead of their gas engine.  Most hybrids have battery packs which are charged by what is called \"generative breaking\".  Instead of slowing down with traditional braking, these cars store some of the energy from braking into a battery pack.  Then when the light changes and the car moves again, the energy stored in the battery can be used to move the car, or assist the engine in moving the car.. . In the city this effect can be more pronounced because of more frequent stopping and starting."),
            403665: GenericDoc(doc_id="1424320_9", text="You can finance up to 100% of the property value. The only kicker is, the terms of your loan may not be as good. Higher interest rates or origination fees may abound. A simple way to figure whether or not this will be a good idea is to calculate your potential income and your expenses and initial sunk costs. This is a gross oversimplification and I would recommend that you speak to a qualified CPA before you make this leap. As usual, I recommend being very conservative in your estimates, assume a few worst case scenarios (ie no rental income between renters) and if the numbers still come out good... then go for it! :) Good luck!. . **EDIT. . I almost forgot to give you a specific loan type!! You can ask for an 80/20 loan. Where you get a traditional loan for 80% of the value with no PMI (private mortgage insurance), and get another 20% loan for the downpayment (this will be at a higher interest rate and you should pay this one down as quickly as possible). Ask your loan officer about this. They may also be able to offer you other alternatives. Good luck! :)"),
        })

    def test_antique_train(self):
        self._test_queries('antique/train', count=2_426, items={
            0: GenericQuery(query_id='3097310', text='What causes severe swelling and pain in the knees?'),
            9: GenericQuery(query_id='992730', text='How do you transfer voicemail messages onto tape?'),
            2425: GenericQuery(query_id='4086230', text='See I have lost my voice what do I do?'),
        })
        self._test_qrels('antique/train', count=27422, items={
            0: TrecQrel(query_id='2531329', doc_id='2531329_0', relevance=4, iteration='U0'),
            9: TrecQrel(query_id='3825668', doc_id='3825668_4', relevance=4, iteration='Q0'),
            27421: TrecQrel(query_id='884731', doc_id='884731_1', relevance=3, iteration='Q0')
        })

    def test_antique_train_split200train(self):
        self._test_queries('antique/train/split200-train', count=2_226, items={
            0: GenericQuery(query_id='3097310', text='What causes severe swelling and pain in the knees?'),
            9: GenericQuery(query_id='3486120', text='Why does PAMELA   ANDERSON ........NOT CARE about  Children?'),
            2225: GenericQuery(query_id='4086230', text='See I have lost my voice what do I do?'),
        })
        self._test_qrels('antique/train/split200-train', count=25229, items={
            0: TrecQrel(query_id='2531329', doc_id='2531329_0', relevance=4, iteration='U0'),
            9: TrecQrel(query_id='3825668', doc_id='3825668_4', relevance=4, iteration='Q0'),
            25228: TrecQrel(query_id='884731', doc_id='884731_1', relevance=3, iteration='Q0')
        })

    def test_antique_train_split200valid(self):
        self._test_queries('antique/train/split200-valid', count=200, items={
            0: GenericQuery(query_id='1907320', text='How do I get college money?'),
            9: GenericQuery(query_id='3083719', text='How do you safely wean a person off Risperidal?'),
            199: GenericQuery(query_id='2573745', text='How did African American women get the right to Vote?'),
        })
        self._test_qrels('antique/train/split200-valid', count=2193, items={
            0: TrecQrel(query_id='2550445', doc_id='2550445_0', relevance=4, iteration='U0'),
            9: TrecQrel(query_id='196651', doc_id='196651_1', relevance=4, iteration='Q0'),
            2192: TrecQrel(query_id='344029', doc_id='344029_4', relevance=4, iteration='Q0')
        })

    def test_antique_test(self):
        self._test_queries("antique/test", count=200, items={
            0: GenericQuery(query_id='3990512', text='how can we get concentration onsomething?'),
            9: GenericQuery(query_id='1783010', text='What is Blaphsemy?'),
            199: GenericQuery(query_id='1971899', text='what is masturbat***?'),
        })
        self._test_qrels('antique/test', count=6589, items={
            0: TrecQrel(query_id='1964316', doc_id='1964316_5', relevance=4, iteration='U0'),
            9: TrecQrel(query_id='1964316', doc_id='1964316_2', relevance=4, iteration='Q0'),
            6588: TrecQrel(query_id='1262692', doc_id='3699008_1', relevance=2, iteration='Q0')
        })

    def test_antique_test_nonoffensive(self):
        self._test_queries('antique/test/non-offensive', count=176, items={
            0: GenericQuery(query_id='3990512', text='how can we get concentration onsomething?'),
            9: GenericQuery(query_id='1783010', text='What is Blaphsemy?'),
            175: GenericQuery(query_id='1340574', text='Why do some people only go to church on Easter Sunday and never go again until Christmas ?')
        })
        self._test_qrels('antique/test/non-offensive', count=5752, items={
            0: TrecQrel(query_id='1964316', doc_id='1964316_5', relevance=4, iteration='U0'),
            9: TrecQrel(query_id='1964316', doc_id='1964316_2', relevance=4, iteration='Q0'),
            5751: TrecQrel(query_id='1262692', doc_id='3699008_1', relevance=2, iteration='Q0')
        })

if __name__ == '__main__':
    unittest.main()
