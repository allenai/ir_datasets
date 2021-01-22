import re
import unittest
from ir_datasets.formats import TrecQrel, TrecDoc, TrecQuery
from .base import DatasetIntegrationTest


class TestTrecRobust04(DatasetIntegrationTest):
    def test_trec_robust04_docs(self):
        self._test_docs('trec-robust04', count=528155, items={
            0: TrecDoc('FBIS3-1', re.compile('^\n\nPOLITICIANS,  PARTY PREFERENCES \n\n   Summary:  Newspapers in the Former Yugoslav Republic of \n   M.{6912}SE CALL CHIEF, \nBALKANS BRANCH AT \\(703\\) 733\\-6481\\) \n\nELAG/25 February/POLCHF/EED/DEW 28/2023Z FEB \n\n\n$', flags=48), re.compile('^<TEXT>\nPOLITICIANS,  PARTY PREFERENCES \n\n   Summary:  Newspapers in the Former Yugoslav Republic of .{6924} CHIEF, \nBALKANS BRANCH AT \\(703\\) 733\\-6481\\) \n\nELAG/25 February/POLCHF/EED/DEW 28/2023Z FEB \n\n</TEXT>\n$', flags=48)),
            9: TrecDoc('FBIS3-10', re.compile('^\n\nHanoi     Finds New Outlet for Surplus Labor \n\n   Judging by a 1 March VNA report, Hanoi has found.{787}TS, PLEASE CALL CHIEF, \nASIA DIVISION ANALYSIS TEAM, \\(703\\) 733\\-6534\\.\\) \nEAG/BIETZ/ta 07/2051z mar \n\n\n$', flags=48), re.compile('^<TEXT>\nHanoi     Finds New Outlet for Surplus Labor \n\n   Judging by a 1 March VNA report, Hanoi has .{799}ASE CALL CHIEF, \nASIA DIVISION ANALYSIS TEAM, \\(703\\) 733\\-6534\\.\\) \nEAG/BIETZ/ta 07/2051z mar \n\n</TEXT>\n$', flags=48)),
            528154: TrecDoc('LA123190-0134', re.compile("^\n\n\nDecember 31, 1990, Monday, P\\.M\\. Final \n\n\n\n\nSHORT TAKES; \n\n\nTAMMY SEES COUNTRY'S REBIRTH \n\n\n\n\nTamm.{547}icky Van Shelton, \nClint Black, Patty Loveless and Garth Brooks as among those making an impact\\. \n\n\n$", flags=48), re.compile('^<DATE>\n<P>\nDecember 31, 1990, Monday, P\\.M\\. Final \n</P>\n</DATE>\n<HEADLINE>\n<P>\nSHORT TAKES; \n</P>\n<P>.{642}elton, \nClint Black, Patty Loveless and Garth Brooks as among those making an impact\\. \n</P>\n</TEXT>\n$', flags=48)),
        })


    def test_trec_robust04_queries(self):
        self._test_queries('trec-robust04', count=250, items={
            0: TrecQuery(query_id='301', title='International Organized Crime', description='Identify organizations that participate in international criminal\nactivity, the activity, and, if possible, collaborating organizations\nand the countries involved.', narrative='A relevant document must as a minimum identify the organization and the\ntype of illegal activity (e.g., Columbian cartel exporting cocaine).\nVague references to international drug trade without identification of\nthe organization(s) involved would not be relevant.'),
            9: TrecQuery(query_id='310', title='Radio Waves and Brain Cancer', description='Evidence that radio waves from radio towers or car phones affect\nbrain cancer occurrence.', narrative='Persons living near radio towers and more recently persons using\ncar phones have been diagnosed with brain cancer.  The argument \nrages regarding the direct association of one with the other.\nThe incidence of cancer among the groups cited is considered, by\nsome, to be higher than that found in the normal population.  A \nrelevant document includes any experiment with animals, statistical \nstudy, articles, news items which report on the incidence of brain \ncancer being higher/lower/same as those persons who live near a \nradio tower and those using car phones as compared to those in the \ngeneral population.'),
            249: TrecQuery(query_id='700', title='gasoline tax U.S.', description='What are the arguments for and against an increase in gasoline\ntaxes in the U.S.?', narrative='Relevant documents present reasons for or against raising gasoline taxes\nin the U.S.  Documents discussing rises or decreases in the price of\ngasoline are not relevant.')
        })
        self._test_queries('trec-robust04/fold1', count=50, items={
            0: TrecQuery(query_id='302', title='Poliomyelitis and Post-Polio', description='Is the disease of Poliomyelitis (polio) under control in the\nworld?', narrative='Relevant documents should contain data or outbreaks of the \npolio disease (large or small scale), medical protection \nagainst the disease, reports on what has been labeled as \n"post-polio" problems.  Of interest would be location of \nthe cases, how severe, as well as what is being done in \nthe "post-polio" area.'),
            9: TrecQuery(query_id='341', title='Airport Security', description='A relevant document would discuss how effective\ngovernment orders to better scrutinize passengers\nand luggage on international flights and to step\nup screening of all carry-on baggage has been.', narrative='A relevant document would contain reports on what\nnew steps airports worldwide have taken to better \nscrutinize passengers and their luggage on \ninternational flights and to step up screening of\nall carry-on baggage.  With the increase in \ninternational terrorism and in the wake of the\nTWA Flight 800 disaster, articles on airport \nsecurity relating in particular to additional\nsteps taken by airports to increase flight safety\nwould be relevant.  The mere mention of enhanced \nsecurity does not constitute relevance.  Additional\nsteps refer to something beyond just passenger\nand carry-on screening using the normal methods.\nExamples of new steps would be additional personnel, \nsophisticated monitoring and screening devices, \nand extraordinary measures to check luggage in the \nbaggage compartment.'),
            49: TrecQuery(query_id='700', title='gasoline tax U.S.', description='What are the arguments for and against an increase in gasoline\ntaxes in the U.S.?', narrative='Relevant documents present reasons for or against raising gasoline taxes\nin the U.S.  Documents discussing rises or decreases in the price of\ngasoline are not relevant.')
        })
        self._test_queries('trec-robust04/fold2', count=50, items={
            0: TrecQuery(query_id='301', title='International Organized Crime', description='Identify organizations that participate in international criminal\nactivity, the activity, and, if possible, collaborating organizations\nand the countries involved.', narrative='A relevant document must as a minimum identify the organization and the\ntype of illegal activity (e.g., Columbian cartel exporting cocaine).\nVague references to international drug trade without identification of\nthe organization(s) involved would not be relevant.'),
            9: TrecQuery(query_id='349', title='Metabolism', description='Document will discuss the chemical reactions \nnecessary to keep living cells healthy and/or\nproducing energy.', narrative='A relevant document will contain specific information\non the catabolic and anabolic reactions of the metabolic\nprocess.  Relevant information includes, but is not \nlimited to, the reactions occurring in metabolism, \nbiochemical processes (Glycolysis or Krebs cycle for\nproduction of energy), and disorders associated with \nthe metabolic rate.'),
            49: TrecQuery(query_id='698', title='literacy rates Africa', description='What are literacy rates in African countries?', narrative='A relevant document will contain information about the\nliteracy rate in an African country.\nGeneral education levels that do not specifically include literacy rates\nare not relevant.')
        })
        self._test_queries('trec-robust04/fold3', count=50, items={
            0: TrecQuery(query_id='306', title='African Civilian Deaths', description='How many civilian non-combatants have been killed in \nthe various civil wars in Africa?', narrative='A relevant document will contain specific casualty \ninformation for a given area, country, or region.  \nIt will cite numbers of civilian deaths caused \ndirectly or indirectly by armed conflict.'),
            9: TrecQuery(query_id='354', title='journalist risks', description='Identify instances where a journalist has been put at risk (e.g.,\nkilled, arrested or taken hostage) in the performance of his work.', narrative='Any document identifying an instance where a journalist or \ncorrespondent has been killed, arrested or taken hostage in the \nperformance of his work is relevant.'),
            49: TrecQuery(query_id='693', title='newspapers electronic media', description='What has been the effect of the electronic media on the newspaper\nindustry?', narrative='Relevant documents must explicitly attribute effects to the electronic\nmedia: information about declining readership is irrelevant unless\nit attributes the cause to the electronic media.')
        })
        self._test_queries('trec-robust04/fold4', count=50, items={
            0: TrecQuery(query_id='320', title='Undersea Fiber Optic Cable', description="Fiber optic link around the globe (Flag) will be\nthe world's longest undersea fiber optic cable.\nWho's involved and how extensive is the technology\non this system.  What problems exist?", narrative='Relevant documents will reference companies involved\nin building the system or the technology needed for\nsuch an endeavor.  Of relevance also would be information\non the link up points of FLAG or landing sites or \ninterconnection with other telecommunication cables.\nRelevant documents may reference any regulatory problems\nwith the system once constructed.  A non-relevant \ndocument would contain information on other fiber optic\nsystems currently in place.'),
            9: TrecQuery(query_id='355', title='ocean remote sensing', description='Identify documents discussing the development and application of\nspaceborne ocean remote sensing.', narrative='Documents discussing the development and application of spaceborne \nocean remote sensing in oceanography, seabed prospecting and \nmining, or any marine-science activity are relevant.  Documents \nthat discuss the application of satellite remote sensing in \ngeography, agriculture, forestry, mining and mineral prospecting \nor any land-bound science are not relevant, nor are references \nto international marketing or promotional advertizing of any \nremote-sensing technology.  Synthetic aperture radar (SAR) \nemployed in ocean remote sensing is relevant.'),
            49: TrecQuery(query_id='697', title='air traffic controller', description='What are working conditions and pay for U.S. air traffic controllers?', narrative='Relevant documents tell something about working conditions\nor pay for American controllers.  Documents about foreign\ncontrollers or an individual controller are not relevant.')
        })
        self._test_queries('trec-robust04/fold5', count=50, items={
            0: TrecQuery(query_id='304', title='Endangered Species (Mammals)', description='Compile a list of mammals that are considered to be endangered,\nidentify their habitat and, if possible, specify what threatens them.', narrative='Any document identifying a mammal as endangered is relevant.  \nStatements of authorities disputing the endangered status would also\nbe relevant.  A document containing information on habitat and\npopulations of a mammal identified elsewhere as endangered would also\nbe relevant even if the document at hand did not identify the species\nas endangered.  Generalized statements about endangered species \nwithout reference to specific mammals would not be relevant.'),
            9: TrecQuery(query_id='339', title="Alzheimer's Drug Treatment", description="What drugs are being used in the treatment of \nAlzheimer's Disease and how successful are they?", narrative="A relevant document should name a drug used in \nthe treatment of Alzheimer's Disease and also \nits manufacturer, and should give some indication \nof the drug's success or failure."),
            49: TrecQuery(query_id='699', title='term limits', description='What are the pros and cons of term limits?', narrative='Relevant documents reflect an opinion on the value of term limits\nwith accompanying reason(s).  Documents that cite the status of term\nlimit legislation or opinions on the issue sans reasons for the opinion\nare not relevant.')
        })

    def test_trec_robust04_qrels(self):
        self._test_qrels('trec-robust04', count=311410, items={
            0: TrecQrel(query_id='301', doc_id='FBIS3-10082', relevance=1, iteration='0'),
            9: TrecQrel(query_id='301', doc_id='FBIS3-10635', relevance=0, iteration='0'),
            311409: TrecQrel(query_id='700', doc_id='LA123090-0137', relevance=0, iteration='0')
        })
        self._test_qrels('trec-robust04/fold1', count=62789, items={
            0: TrecQrel(query_id='302', doc_id='FBIS3-10615', relevance=0, iteration='0'),
            9: TrecQrel(query_id='302', doc_id='FBIS3-22470', relevance=0, iteration='0'),
            62788: TrecQrel(query_id='700', doc_id='LA123090-0137', relevance=0, iteration='0')
        })
        self._test_qrels('trec-robust04/fold2', count=63917, items={
            0: TrecQrel(query_id='301', doc_id='FBIS3-10082', relevance=1, iteration='0'),
            9: TrecQrel(query_id='301', doc_id='FBIS3-10635', relevance=0, iteration='0'),
            63916: TrecQrel(query_id='698', doc_id='LA123190-0100', relevance=0, iteration='0')
        })
        self._test_qrels('trec-robust04/fold3', count=62901, items={
            0: TrecQrel(query_id='306', doc_id='FBIS3-1010', relevance=0, iteration='0'),
            9: TrecQrel(query_id='306', doc_id='FBIS3-13331', relevance=0, iteration='0'),
            62900: TrecQrel(query_id='693', doc_id='LA122789-0115', relevance=0, iteration='0')
        })
        self._test_qrels('trec-robust04/fold4', count=57962, items={
            0: TrecQrel(query_id='320', doc_id='FBIS3-10291', relevance=0, iteration='0'),
            9: TrecQrel(query_id='320', doc_id='FBIS3-20327', relevance=0, iteration='0'),
            57961: TrecQrel(query_id='697', doc_id='LA122589-0068', relevance=0, iteration='0')
        })
        self._test_qrels('trec-robust04/fold5', count=63841, items={
            0: TrecQrel(query_id='304', doc_id='FBIS3-1584', relevance=0, iteration='0'),
            9: TrecQrel(query_id='304', doc_id='FBIS3-37947', relevance=0, iteration='0'),
            63840: TrecQrel(query_id='699', doc_id='LA123190-0008', relevance=0, iteration='0')
        })


if __name__ == '__main__':
    unittest.main()
