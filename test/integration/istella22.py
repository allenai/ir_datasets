import re
import unittest
import ir_datasets
from ir_datasets.formats import GenericQuery, TrecQrel
from ir_datasets.datasets.istella22 import Istella22Doc
from .base import DatasetIntegrationTest


class TestIstella22(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('istella22', count=8421456, items={
            0: Istella22Doc('1990010000000002', 'Play Online Roulette 2016 - #1 Best Online Roulette Casinos!', 'http://www.onlineroulette.org/', re.compile('^The benefits of online roulette often outweigh live roulette in a number of different ways\\. There’s .{6411}Play Now Casino Rating Payout 4\\.8 /5 98\\.29% 4\\.7 /5 97\\.19% 4\\.0 /5 97\\.29% 3\\.9 /5 95\\.05% Bonus 1 2 3 4 $', flags=48), re.compile('^\ufeff Play Online Roulette 2016 \\- \\#1 Best Online Roulette Casinos! Responsive, accurate customer support.{1029}i Qatar Deutschland Nederland French España Italia Português Canadien Home Real Money About Sitemap $', flags=48), 'en', 92),
            9: Istella22Doc('1990010000000016', 'HowOpenSource', 'http://www.howopensource.com/', re.compile("^This website uses cookies to improve your experience\\. We'll assume you're ok with this, but you can .{4532}t have Firefox Add\\-ons How To Add User To Ubuntu Simple backup to DropBox Select Page Page 1 of 3 1 $", flags=48), re.compile('^HowOpenSource How To Install Linux Error Python Today’s Wisdom Chrome Firefox WordPress Plugins Some.{13039}3\\.04, Ubuntu 12\\.10 / 12\\. 04 / 11\\.10 / 11\\.04 using\\.\\.\\. 2 3 » Home About Privacy Cookie Policy Contact $', flags=48), 'en', 97),
            8421455: Istella22Doc('1990159902674778', 'Comune di Nuragus (CA) - Italia: Informazioni', 'http://www.comuni-italiani.it/092/115/', re.compile("^Per segnalare aggiunte o correzioni da effettuare sulla scheda del comune di Nuragus, inviaci un'ema.{7}a: questo non è l'indirizzo email del comune\\) Comune di Nuragus Informativa Privacy \\- Note sui Dati $", flags=48), re.compile('^Comune di Nuragus \\(CA\\) \\- Italia: Informazioni Comune di Nuragus \\(Provincia di Cagliari, Regione Sard.{1619}Prometheo  Comuni Provincia di Cagliari: Comune di Nurallao Comune di Muravera Lista Nuragus, Italy $', flags=48), 'it', 95),
        })

    def test_queries(self):
        self._test_queries('istella22/test', count=2198, items={
            0: GenericQuery('263', 'calcio mercato'),
            9: GenericQuery('1008', 'milano finanza'),
            2197: GenericQuery('90118', 'abbigliamento per parrucchieri'),
        })
        self._test_queries('istella22/test/fold1', count=440, items={
            0: GenericQuery('480', 'giallo zafferano'),
            9: GenericQuery('4797', 'apple store'),
            439: GenericQuery('89903', 'agevolazione pagamenti tasse ai disabili'),
        })
        self._test_queries('istella22/test/fold2', count=440, items={
            0: GenericQuery('594', 'mediaset video'),
            9: GenericQuery('5599', 'informazione scorretta'),
            439: GenericQuery('89898', 'aggiornare navigon'),
        })
        self._test_queries('istella22/test/fold3', count=440, items={
            0: GenericQuery('263', 'calcio mercato'),
            9: GenericQuery('6516', 'attrezzature di saldatura'),
            439: GenericQuery('90118', 'abbigliamento per parrucchieri'),
        })
        self._test_queries('istella22/test/fold4', count=439, items={
            0: GenericQuery('788', 'gianna nannini'),
            9: GenericQuery('5166', 'ultime notizie'),
            438: GenericQuery('88988', 'avvelenamento da acqua'),
        })
        self._test_queries('istella22/test/fold5', count=439, items={
            0: GenericQuery('326', 'cotto e mangiato'),
            9: GenericQuery('3079', 'quote snai'),
            438: GenericQuery('89691', 'allarmi fai da te'),
        })

    def test_qrels(self):
        self._test_qrels('istella22/test', count=10693, items={
            0: TrecQrel('263', '1990028700044315', 3, '0'),
            9: TrecQrel('326', '1990066502519695', 3, '0'),
            10692: TrecQrel('90118', '1990158300000425', 3, '0'),
        })
        self._test_qrels('istella22/test/fold1', count=2164, items={
            0: TrecQrel('480', '1990024100001149', 4, '0'),
            9: TrecQrel('2519', '1990060100096961', 1, '0'),
            2163: TrecQrel('89903', '1990096101187019', 1, '0'),
        })
        self._test_qrels('istella22/test/fold2', count=2140, items={
            0: TrecQrel('594', '1990081700000321', 3, '0'),
            9: TrecQrel('680', '1990046900019247', 3, '0'),
            2139: TrecQrel('89898', '1990130400118331', 3, '0'),
        })
        self._test_qrels('istella22/test/fold3', count=2197, items={
            0: TrecQrel('263', '1990028700044315', 3, '0'),
            9: TrecQrel('1498', '1990035702542094', 3, '0'),
            2196: TrecQrel('90118', '1990158300000425', 3, '0'),
        })
        self._test_qrels('istella22/test/fold4', count=2098, items={
            0: TrecQrel('788', '1990058802543970', 1, '0'),
            9: TrecQrel('1421', '1990110302541381', 1, '0'),
            2097: TrecQrel('88988', '1990017202541615', 1, '0'),
        })
        self._test_qrels('istella22/test/fold5', count=2094, items={
            0: TrecQrel('326', '1990022900152987', 3, '0'),
            9: TrecQrel('381', '1990123900000072', 3, '0'),
            2093: TrecQrel('89691', '1990153102541850', 1, '0'),
        })


if __name__ == '__main__':
    unittest.main()
