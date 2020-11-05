import re
import unittest
from ir_datasets.datasets.trec_mandarin import TrecMandarinQuery
from ir_datasets.formats import TrecQrel, TrecDoc
from .base import DatasetIntegrationTest


class TestTrecMandarin(DatasetIntegrationTest):
    def test_trec_mandarin_docs(self):
        self._test_docs('trec-mandarin', count=164789, items={
            0: TrecDoc('pd9312-1', re.compile('^\n\\#1 1993年12月01日\\#2 1\\#3 要闻\\#4 结束美国古巴巴西葡萄牙之行\\#4 江泽民主席回到北京\\#4 李鹏乔石刘华清荣毅仁等在人民大会堂迎接\\#4 祝贺江主席出访达到和平谅解合作互利目的\\#5 吴.{669}解和友谊。”他表示相信，在双方共同努力下，中葡两国友好合作关系必将发展到一个新的水平。\n\n陪同江泽民主席出访的国务院副总理兼外交部长钱其琛，特别助理曾庆红、杨德中，外交部副部长刘华秋等也同机离开。\n\n$', flags=48), re.compile('^<HL>\\#1 1993年12月01日\\#2 1\\#3 要闻\\#4 结束美国古巴巴西葡萄牙之行\\#4 江泽民主席回到北京\\#4 李鹏乔石刘华清荣毅仁等在人民大会堂迎接\\#4 祝贺江主席出访达到和平谅解合作互利目的\\#.{690}表示相信，在双方共同努力下，中葡两国友好合作关系必将发展到一个新的水平。\n\n陪同江泽民主席出访的国务院副总理兼外交部长钱其琛，特别助理曾庆红、杨德中，外交部副部长刘华秋等也同机离开。\n</TEXT>\n$', flags=48)),
            9: TrecDoc('pd9312-10', '\n\n�#1 1993年12月01日#2 1#3 要闻#4 江泽民主席返回北京李鹏、乔石、刘华清、荣毅仁等在人民大会堂迎接#5 刘建国\n\n\n江泽民主席返回北京，李鹏、乔石、刘华清、荣毅仁等在北京人民大会堂迎接。\n\n新华社记者刘建国摄\n\n', '<DOC>\n\n<HL>�#1 1993年12月01日#2 1#3 要闻#4 江泽民主席返回北京李鹏、乔石、刘华清、荣毅仁等在人民大会堂迎接#5 刘建国</HL>\n<TEXT>\n\n江泽民主席返回北京，李鹏、乔石、刘华清、荣毅仁等在北京人民大会堂迎接。\n\n新华社记者刘建国摄\n</TEXT>\n'),
            164788: TrecDoc('CB049030-BFW-744-737', re.compile('^\n\n CB049030\\.BFW \\(  744\\)     \n     1994\\-05\\-01 00:40:54 \\(5\\) \n\n 李鹏参加内蒙古庆祝“五一”国际劳动节活动 \n\n 新华社呼和浩特四月三十日电（记.{654}节文艺晚会，观看了具有浓郁民族风格的精彩文艺演出。 \n 晚会结束时，李鹏总理走上舞台，穿上蒙古服装，与演员们合影。 \n\n\n 司马义·艾买提和叶青、姚振炎、韩杼滨等也出席了文艺晚会。 \n （完） \n\n\n$', flags=48), re.compile('^<DOC>\n<DOCID> CB049030\\.BFW \\(  744\\)     </DOCID>\n<DATE>     1994\\-05\\-01 00:40:54 \\(5\\) </DATE>\n<TEXT>\n<h.{881}总理走上舞台，穿上蒙古服装，与演员们合影。 </s>\n</p>\n<p>\n<s> 司马义·艾买提和叶青、姚振炎、韩杼滨等也出席了文艺晚会。 </s>\n<s> （完） </s>\n</p>\n</TEXT>\n$', flags=48)),
        })

    def test_trec_mandarin_queries(self):
        self._test_queries('trec-mandarin/trec5', count=28, items={
            0: TrecMandarinQuery(query_id='1', title_en='U.S. to separate the most-favored-nation status from human rights issue in China.', title_zh='美国决定将中国大陆的人权状况与其是否给予中共最惠国待\u3000遇分离．', description_en='most-favored nation status, human rights in\nChina, economic sanctions, separate, untie', description_zh='最惠国待遇，中国，人权，经济制裁，分离，脱钩', narrative_en='A relevant document should describe why the U.S.\nseparates most-favored nation status from \nhuman rights. A relevant document should \nalso mention why China opposes U.S. attempts\nto tie human rights to most-favored-nation\nstatus.', narrative_zh='相关文件必须提到美国为何将最惠国待遇与人权分离；\n\u3000相关文件也必须提到中共为什么反对美国将人权与最\n\u3000\u3000\u3000惠国待遇相提并论．'),
            9: TrecMandarinQuery(query_id='10', title_en='Border Trade in Xinjiang', title_zh='新疆的边境贸易', description_en='Xinjiang, Uigur, border trade, market,', description_zh='新疆,维吾尔,边境贸易,边贸,市场', narrative_en='A relevant document should contain information on\nthe trading relationship between Xinjiang, China              \nand its neighboring nations, including  treaties            \nsigned by China and former Soviet Republics\nthat are bordering China and foreign investment.  \nIf a document contains information on how China\ndevelops Xinjiang, it is not relevant.', narrative_zh='相关文件必须包括中国新疆与其邻近国家的贸易关系,此关系包括\n中国与前苏联共和国之间所签署的贸易条约以及彼此间的外贸投\n资.如果文件只论及中国如何建设发展新疆，则属非相关文件.'),
            27: TrecMandarinQuery(query_id='28', title_en='The Spread of Cellular Phones in China', title_zh='移动电话在中国的成长', description_en='digital, cellular, cellular phone, net, automatic roaming', description_zh='数字,蜂窝式,移动电话,网络,自动漫游', narrative_en='A relevant document contains the following kinds of information:  \nthe number of cellular phone users, area coverage, or how PSDN  \nis implemented for national cellular communication.  A non-relevant \ndocument includes reports on commercial manufacturers or brand name \ncellular phones.', narrative_zh='相关文件应包括下列信息: 中国移动电话用户数,\n   覆盖地区, 中国如何以数据分组交换网覆盖\n   全国移动电话的通讯. 不相关文件则包括 有关\n   制造移动电话厂商的报道, 以及移动电话的\n   厂牌.')
        })
        self._test_queries('trec-mandarin/trec6', count=26, items={
            0: TrecMandarinQuery(query_id='29', title_en='Building the Information Super Highway', title_zh='信息高速公路的建设', description_en='Information Super Highway, building', description_zh='信息高速公路，建设', narrative_en='A relevant document should discuss  building the Information Super Highway, including any technical problems, problems with the  information infrastructure, or plans for use of the Internet by developed or developing countries.', narrative_zh='相关文件应提到信息高速公路的建设，包括任何技术上的，或与信息基础设施有关的问题，以及有关发达国家或发展中国家对国际网络的应用计划．'),
            9: TrecMandarinQuery(query_id='38', title_en='Protection of Wildlife in China', title_zh='中国野生动物保护形势', description_en='Protection of Wildlife, Legislation Protecting Wildlife, Associations for the Protection of Wildlife, rare and precious animals, endangered species', description_zh='野生动物保护，《野生动物保护法》，野生动物保护协会，珍稀动物，濒危动物', narrative_en='A relevant document should discuss protection of endangered species in China. Relevant documents include the following information: (1) "Legislation protecting endangered species", (2)\u3000rare and precious animals, (3) hunting and selling wild animals, (4) adopting measures to rescue rare animals, (5) market survelliance work, or (6) establishing preservation grounds for endangered species.', narrative_zh='相关文件应提到中国野生动物保护形势．相关文件包括下列信息:（一）《野生动物保护法》，（二）珍稀动物，（三）捕猎和销售野生动物，（四）采取措施抢救珍稀动物，（五）市场管制工作，或（六）建设濒危动物的保护区.'),
            25: TrecMandarinQuery(query_id='54', title_en="China's Reaction to U.S.  Sale of F-16 Fighters to Taiwan", title_zh='中国关于美国政府向台湾出售Ｆ－１６战斗机的反应', description_en='China, U.S., Taiwan, F-16 fighter, sale', description_zh='中国，美国，台湾，Ｆ—１６战斗机，出售', narrative_en='A relevant document should discuss the resolution concerning U.S. weapon sales to Taiwan in the Sino-American "8-17" Joint Communique and why the Chinese consider President Bush\'s decision to sell F-16 fighters to Taiwan to be in violation of the spirit of the  Sino-American "8-17" Joint Communique and to be damaging to Sino-American relations.', narrative_zh='相关文件应提到中美“八·一七”联合公报中对美国向台湾出售武器之决定，以及为何中国认为布什总统决定售予台湾Ｆ—１６战斗机是违反中美“八·一七”联合公报之精神并损害中美关系．')
        })

    def test_trec_mandarin_qrels(self):
        self._test_qrels('trec-mandarin/trec5', count=15588, items={
            0: TrecQrel(query_id='1', doc_id='CB001007-BFJ-588-408', relevance=0, iteration='0'),
            9: TrecQrel(query_id='1', doc_id='CB006019-BFJ-2117-506', relevance=0, iteration='0'),
            15587: TrecQrel(query_id='28', doc_id='pd9312-91', relevance=0, iteration='0')
        })
        self._test_qrels('trec-mandarin/trec6', count=9236, items={
            0: TrecQrel(query_id='29', doc_id='CB001004-BFW-1143-212', relevance=1, iteration='0'),
            9: TrecQrel(query_id='29', doc_id='CB002028-BFW-1086-1035', relevance=0, iteration='0'),
            9235: TrecQrel(query_id='54', doc_id='pd9312-1824', relevance=0, iteration='0')
        })


if __name__ == '__main__':
    unittest.main()
