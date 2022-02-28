import re
import unittest
from ir_datasets.datasets.cranfield import CranfieldDoc
from ir_datasets.formats import TrecQrel, TrecParsedDoc, TrecQuery
from .base import DatasetIntegrationTest


class TestDisks45(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('disks45/nocr', count=528155, items={
            0: TrecParsedDoc('FBIS3-1', 'FORMER YUGOSLAV REPUBLIC OF MACEDONIA: OPINION POLLS ON', re.compile('^POLITICIANS, PARTY PREFERENCES\nSummary: Newspapers in the Former Yugoslav Republic of\nMacedonia have.{5410}S, PLEASE CALL CHIEF,\nBALKANS BRANCH AT \\(703\\) 733\\-6481\\)\nELAG/25 February/POLCHF/EED/DEW 28/2023Z FEB$', flags=48), re.compile(b'^<DOC>\n<DOCNO> FBIS3\\-1 </DOCNO>\n<HT>  "cr00000011094001" </HT>\n\n\n<HEADER>\n<H2>   March Reports </H2>\n.{7224} \nBALKANS BRANCH AT \\(703\\) 733\\-6481\\) \n\nELAG/25 February/POLCHF/EED/DEW 28/2023Z FEB \n\n</TEXT>\n\n</DOC>$', flags=16)),
            9: TrecParsedDoc('FBIS3-10', 'Vietnam-Libya', re.compile('^Hanoi Finds New Outlet for Surplus Labor\nJudging by a 1 March VNA report, Hanoi has found new\nopport.{751}COMMENTS, PLEASE CALL CHIEF,\nASIA DIVISION ANALYSIS TEAM, \\(703\\) 733\\-6534\\.\\)\nEAG/BIETZ/ta 07/2051z mar$', flags=48), re.compile(b'^<DOC>\n<DOCNO> FBIS3\\-10 </DOCNO>\n<HT>    "cr00000011994001" </HT>\n\n\n<HEADER>\n<DATE1>   9 March 1994.{1274}L CHIEF, \nASIA DIVISION ANALYSIS TEAM, \\(703\\) 733\\-6534\\.\\) \nEAG/BIETZ/ta 07/2051z mar \n\n</TEXT>\n\n</DOC>$', flags=16)),
            528154: TrecParsedDoc('LA123190-0134', "SHORT TAKES;\nTAMMY SEES COUNTRY'S REBIRTH", re.compile('^December 31, 1990, Monday, P\\.M\\. Final\nTammy Wynette says a new generation of performers has helped p.{470}is, Ricky Van Shelton,\nClint Black, Patty Loveless and Garth Brooks as among those making an impact\\.$', flags=48), re.compile(b'^<DOC>\n<DOCNO> LA123190\\-0134 </DOCNO>\n<DOCID> 329701 </DOCID>\n<DATE>\n<P>\nDecember 31, 1990, Monday, .{972}th Brooks as among those making an impact\\. \n</P>\n</TEXT>\n<TYPE>\n<P>\nBrief; Wire \n</P>\n</TYPE>\n</DOC>$', flags=16)),
        })

    def test_queries(self):
        self._test_queries('disks45/nocr/trec-robust-2004', count=250, items={
            0: TrecQuery('301', 'International Organized Crime', 'Identify organizations that participate in international criminal\nactivity, the activity, and, if possible, collaborating organizations\nand the countries involved.', 'A relevant document must as a minimum identify the organization and the\ntype of illegal activity (e.g., Columbian cartel exporting cocaine).\nVague references to international drug trade without identification of\nthe organization(s) involved would not be relevant.'),
            9: TrecQuery('310', 'Radio Waves and Brain Cancer', 'Evidence that radio waves from radio towers or car phones affect\nbrain cancer occurrence.', 'Persons living near radio towers and more recently persons using\ncar phones have been diagnosed with brain cancer.  The argument \nrages regarding the direct association of one with the other.\nThe incidence of cancer among the groups cited is considered, by\nsome, to be higher than that found in the normal population.  A \nrelevant document includes any experiment with animals, statistical \nstudy, articles, news items which report on the incidence of brain \ncancer being higher/lower/same as those persons who live near a \nradio tower and those using car phones as compared to those in the \ngeneral population.'),
            249: TrecQuery('700', 'gasoline tax U.S.', 'What are the arguments for and against an increase in gasoline\ntaxes in the U.S.?', 'Relevant documents present reasons for or against raising gasoline taxes\nin the U.S.  Documents discussing rises or decreases in the price of\ngasoline are not relevant.'),
        })
        self._test_queries('disks45/nocr/trec-robust-2004/fold1', count=50, items={
            0: TrecQuery('302', 'Poliomyelitis and Post-Polio', 'Is the disease of Poliomyelitis (polio) under control in the\nworld?', 'Relevant documents should contain data or outbreaks of the \npolio disease (large or small scale), medical protection \nagainst the disease, reports on what has been labeled as \n"post-polio" problems.  Of interest would be location of \nthe cases, how severe, as well as what is being done in \nthe "post-polio" area.'),
            9: TrecQuery('341', 'Airport Security', 'A relevant document would discuss how effective\ngovernment orders to better scrutinize passengers\nand luggage on international flights and to step\nup screening of all carry-on baggage has been.', 'A relevant document would contain reports on what\nnew steps airports worldwide have taken to better \nscrutinize passengers and their luggage on \ninternational flights and to step up screening of\nall carry-on baggage.  With the increase in \ninternational terrorism and in the wake of the\nTWA Flight 800 disaster, articles on airport \nsecurity relating in particular to additional\nsteps taken by airports to increase flight safety\nwould be relevant.  The mere mention of enhanced \nsecurity does not constitute relevance.  Additional\nsteps refer to something beyond just passenger\nand carry-on screening using the normal methods.\nExamples of new steps would be additional personnel, \nsophisticated monitoring and screening devices, \nand extraordinary measures to check luggage in the \nbaggage compartment.'),
            49: TrecQuery('700', 'gasoline tax U.S.', 'What are the arguments for and against an increase in gasoline\ntaxes in the U.S.?', 'Relevant documents present reasons for or against raising gasoline taxes\nin the U.S.  Documents discussing rises or decreases in the price of\ngasoline are not relevant.'),
        })
        self._test_queries('disks45/nocr/trec-robust-2004/fold2', count=50, items={
            0: TrecQuery('301', 'International Organized Crime', 'Identify organizations that participate in international criminal\nactivity, the activity, and, if possible, collaborating organizations\nand the countries involved.', 'A relevant document must as a minimum identify the organization and the\ntype of illegal activity (e.g., Columbian cartel exporting cocaine).\nVague references to international drug trade without identification of\nthe organization(s) involved would not be relevant.'),
            9: TrecQuery('349', 'Metabolism', 'Document will discuss the chemical reactions \nnecessary to keep living cells healthy and/or\nproducing energy.', 'A relevant document will contain specific information\non the catabolic and anabolic reactions of the metabolic\nprocess.  Relevant information includes, but is not \nlimited to, the reactions occurring in metabolism, \nbiochemical processes (Glycolysis or Krebs cycle for\nproduction of energy), and disorders associated with \nthe metabolic rate.'),
            49: TrecQuery('698', 'literacy rates Africa', 'What are literacy rates in African countries?', 'A relevant document will contain information about the\nliteracy rate in an African country.\nGeneral education levels that do not specifically include literacy rates\nare not relevant.'),
        })
        self._test_queries('disks45/nocr/trec-robust-2004/fold3', count=50, items={
            0: TrecQuery('306', 'African Civilian Deaths', 'How many civilian non-combatants have been killed in \nthe various civil wars in Africa?', 'A relevant document will contain specific casualty \ninformation for a given area, country, or region.  \nIt will cite numbers of civilian deaths caused \ndirectly or indirectly by armed conflict.'),
            9: TrecQuery('354', 'journalist risks', 'Identify instances where a journalist has been put at risk (e.g.,\nkilled, arrested or taken hostage) in the performance of his work.', 'Any document identifying an instance where a journalist or \ncorrespondent has been killed, arrested or taken hostage in the \nperformance of his work is relevant.'),
            49: TrecQuery('693', 'newspapers electronic media', 'What has been the effect of the electronic media on the newspaper\nindustry?', 'Relevant documents must explicitly attribute effects to the electronic\nmedia: information about declining readership is irrelevant unless\nit attributes the cause to the electronic media.'),
        })
        self._test_queries('disks45/nocr/trec-robust-2004/fold4', count=50, items={
            0: TrecQuery('320', 'Undersea Fiber Optic Cable', "Fiber optic link around the globe (Flag) will be\nthe world's longest undersea fiber optic cable.\nWho's involved and how extensive is the technology\non this system.  What problems exist?", 'Relevant documents will reference companies involved\nin building the system or the technology needed for\nsuch an endeavor.  Of relevance also would be information\non the link up points of FLAG or landing sites or \ninterconnection with other telecommunication cables.\nRelevant documents may reference any regulatory problems\nwith the system once constructed.  A non-relevant \ndocument would contain information on other fiber optic\nsystems currently in place.'),
            9: TrecQuery('355', 'ocean remote sensing', 'Identify documents discussing the development and application of\nspaceborne ocean remote sensing.', 'Documents discussing the development and application of spaceborne \nocean remote sensing in oceanography, seabed prospecting and \nmining, or any marine-science activity are relevant.  Documents \nthat discuss the application of satellite remote sensing in \ngeography, agriculture, forestry, mining and mineral prospecting \nor any land-bound science are not relevant, nor are references \nto international marketing or promotional advertizing of any \nremote-sensing technology.  Synthetic aperture radar (SAR) \nemployed in ocean remote sensing is relevant.'),
            49: TrecQuery('697', 'air traffic controller', 'What are working conditions and pay for U.S. air traffic controllers?', 'Relevant documents tell something about working conditions\nor pay for American controllers.  Documents about foreign\ncontrollers or an individual controller are not relevant.'),
        })
        self._test_queries('disks45/nocr/trec-robust-2004/fold5', count=50, items={
            0: TrecQuery('304', 'Endangered Species (Mammals)', 'Compile a list of mammals that are considered to be endangered,\nidentify their habitat and, if possible, specify what threatens them.', 'Any document identifying a mammal as endangered is relevant.  \nStatements of authorities disputing the endangered status would also\nbe relevant.  A document containing information on habitat and\npopulations of a mammal identified elsewhere as endangered would also\nbe relevant even if the document at hand did not identify the species\nas endangered.  Generalized statements about endangered species \nwithout reference to specific mammals would not be relevant.'),
            9: TrecQuery('339', "Alzheimer's Drug Treatment", "What drugs are being used in the treatment of \nAlzheimer's Disease and how successful are they?", "A relevant document should name a drug used in \nthe treatment of Alzheimer's Disease and also \nits manufacturer, and should give some indication \nof the drug's success or failure."),
            49: TrecQuery('699', 'term limits', 'What are the pros and cons of term limits?', 'Relevant documents reflect an opinion on the value of term limits\nwith accompanying reason(s).  Documents that cite the status of term\nlimit legislation or opinions on the issue sans reasons for the opinion\nare not relevant.'),
        })
        self._test_queries('disks45/nocr/trec7', count=50, items={
            0: TrecQuery('351', 'Falkland petroleum exploration', 'What information is available on petroleum exploration in \nthe South Atlantic near the Falkland Islands?', 'Any document discussing petroleum exploration in the\nSouth Atlantic near the Falkland Islands is considered\nrelevant.  Documents discussing petroleum exploration in \ncontinental South America are not relevant.'),
            9: TrecQuery('360', 'drug legalization benefits', 'What are the benefits, if any, of drug legalization?', 'Relevant documents may contain information on perceived\nbenefits of drug legalization, such as crime reduction, \nimproved treatment using monies which otherwise would \nhave gone for crime fighting, reduced drug addiction, \nand increased governmental income.  Documents that \ndiscuss drug legalization and whether legalization\nis or is not perceived to be beneficial are relevant.'),
            49: TrecQuery('400', 'Amazon rain forest', 'What measures are being taken by local South\nAmerican authorities to preserve the Amazon\ntropical rain forest?', 'Relevant documents may identify: the official \norganizations, institutions, and individuals\nof the countries included in the Amazon rain\nforest; the measures being taken by them to\npreserve the rain forest; and indications of\ndegrees of success in these endeavors.'),
        })
        self._test_queries('disks45/nocr/trec8', count=50, items={
            0: TrecQuery('401', 'foreign minorities, Germany', 'What language and cultural differences impede the integration \nof foreign minorities in Germany?', 'A relevant document will focus on the causes of the lack of\nintegration in a significant way; that is, the mere mention of\nimmigration difficulties is not relevant.  Documents that discuss\nimmigration problems unrelated to Germany are also not relevant.'),
            9: TrecQuery('410', 'Schengen agreement', 'Who is involved in the Schengen agreement to eliminate border\ncontrols in Western Europe and what do they hope to accomplish?', 'Relevant documents will contain any information about the\nactions of signatories of the Schengen agreement such as:\nmeasures to eliminate border controls (removal of traffic\nobstacles, lifting of traffic restrictions); implementation\nof the information system data bank that contains unified\nvisa issuance procedures; or strengthening of border controls\nat the external borders of the treaty area in exchange for \nfree movement at the internal borders.  Discussions of border \ncrossovers for business purposes are not relevant.'),
            49: TrecQuery('450', 'King Hussein, peace', 'How significant a figure over the years was the late\nJordanian King Hussein in furthering peace in the \nMiddle East?', "A relevant document must include mention of Israel;\nKing Hussein himself as opposed to other Jordanian\nofficials; discussion of the King's on-going, previous \nor upcoming efforts; and efforts pertinent to the peace \nprocess, not merely Jordan's relationship with other \nmiddle-east countries or the U.S."),
        })

    def test_qrels(self):
        self._test_qrels('disks45/nocr/trec-robust-2004', count=311410, items={
            0: TrecQrel('301', 'FBIS3-10082', 1, '0'),
            9: TrecQrel('301', 'FBIS3-10635', 0, '0'),
            311409: TrecQrel('700', 'LA123090-0137', 0, '0'),
        })
        self._test_qrels('disks45/nocr/trec-robust-2004/fold1', count=62789, items={
            0: TrecQrel('302', 'FBIS3-10615', 0, '0'),
            9: TrecQrel('302', 'FBIS3-22470', 0, '0'),
            62788: TrecQrel('700', 'LA123090-0137', 0, '0'),
        })
        self._test_qrels('disks45/nocr/trec-robust-2004/fold2', count=63917, items={
            0: TrecQrel('301', 'FBIS3-10082', 1, '0'),
            9: TrecQrel('301', 'FBIS3-10635', 0, '0'),
            63916: TrecQrel('698', 'LA123190-0100', 0, '0'),
        })
        self._test_qrels('disks45/nocr/trec-robust-2004/fold3', count=62901, items={
            0: TrecQrel('306', 'FBIS3-1010', 0, '0'),
            9: TrecQrel('306', 'FBIS3-13331', 0, '0'),
            62900: TrecQrel('693', 'LA122789-0115', 0, '0'),
        })
        self._test_qrels('disks45/nocr/trec-robust-2004/fold4', count=57962, items={
            0: TrecQrel('320', 'FBIS3-10291', 0, '0'),
            9: TrecQrel('320', 'FBIS3-20327', 0, '0'),
            57961: TrecQrel('697', 'LA122589-0068', 0, '0'),
        })
        self._test_qrels('disks45/nocr/trec-robust-2004/fold5', count=63841, items={
            0: TrecQrel('304', 'FBIS3-1584', 0, '0'),
            9: TrecQrel('304', 'FBIS3-37947', 0, '0'),
            63840: TrecQrel('699', 'LA123190-0008', 0, '0'),
        })
        self._test_qrels('disks45/nocr/trec7', count=80345, items={
            0: TrecQrel('351', 'FBIS3-10411', 0, '0'),
            9: TrecQrel('351', 'FBIS3-11107', 1, '0'),
            80344: TrecQrel('400', 'LA123190-0051', 0, '0'),
        })
        self._test_qrels('disks45/nocr/trec8', count=86830, items={
            0: TrecQrel('401', 'FBIS3-10009', 0, '0'),
            9: TrecQrel('401', 'FBIS3-11424', 0, '0'),
            86829: TrecQrel('450', 'LA123190-0061', 0, '0'),
        })


if __name__ == '__main__':
    unittest.main()
