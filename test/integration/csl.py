import re
import unittest
from ir_datasets.formats import TrecQrel, ExctractedCCNoReportQuery
from ir_datasets.datasets.csl import CslDoc
from .base import DatasetIntegrationTest


class TestCsl(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('csl/trec-2023', count=395927, items={
            0: CslDoc('csl-387565', '谈若敖氏', re.compile('^"\'若敖之鬼馁而\',也是一件人生的大哀",这句话是说阿Q需要一个女人的念头不仅合符礼制:所谓"不孝有三,无后为大";而且也是十分现实的事情,即自己饿死了尚不要紧,连祖先也没有人供奉了则是一件大事\\.其实.{7}冠冕堂皇,阿Q私心至多不过这么想:若没有一个女人,"断子绝孙便没有人供一碗饭"\\.自然这就同"若敖之鬼馁而"的境遇一样了\\.当然无论阿Q是否知书识礼,此话用在阿Q身上就成了歪理,不过歪理对于阿Q就是真理\\.$', flags=48), ['女人', '真理', '人生', '礼制', '境遇'], '文学', 'Literature', '中国语言文学', 'Chinese Literature'),
            9: CslDoc('csl-153711', '图书馆的"计算机2000年问题"及解决措施', re.compile('^"2000年问题"是由于早期编程人员为了节省存储空间,用年代的后两位数字代替四位计年而造成的\\.就是说随着人类迈入2000年,某些计算机系统将会出现一片混乱\\.首先软件计数将会转化成"00",这和代表19.{17}可能会自动初始化\\.例如1997年用97表示,简单方便,但1900年用00表示,而2000年的表示方式仍然是"00",这就导致了系统在处理与日期相关的问题时,出现一系列的错误\\.这就是"2000年问题"\\.$', flags=48), ['图书馆', '计算机系统', '2000年问题', '存储空间', '表示方式', '初始化', '转化', '硬件', '四位', '数字', '软件', '人员', '计数', '处理', '编程'], '管理学', 'Management', '图书馆、情报与档案管理', 'Library Information and Archives Management'),
            395926: CslDoc('csl-042248', '少数民族理科学习困境的因素分析', re.compile('^＿＿摘＿理科教育质量事关少数民族学生认知发展、就业前途以及社会和谐与稳定。本文分析影响少数民族理科教育质量的主要因素：民族地区语言－教学模式处于新的探索阶段，理科教师的语言转换能力难度较大，理科课程标.{63}教育需定位于培养学生的基本科学素养，以及解决生产生活问题的实际能力。建议开设“语言与文化适宜的教学法”等课程，提高民族地区教师的教学能力和语言转化能力；加强基于教育与心理的实证研究与理科教学实践探索。$', flags=48), ['少数民族', '语言-教学模式', '理科学习', '语言和文化适宜'], '教育学', 'Pedagogy', '教育学', 'Pedagogy'),
        })

    def test_queries(self):
        self._test_queries('csl/trec-2023', count=41, items={
            0: ExctractedCCNoReportQuery('1', 'human reproductive system', 'I am looking for articles describing the human reproductive system.', 'Articles describing the human reproductive system in biological terms from the perspective of a modern biologist are considered relevant. Articles describing the overall system such as a general description of the components or a lower level description such as specific cellular or molecular pathways involved in either the early development or adult stage maintenance of the systemo are relevant. Descriptions of mammalian reproductive system in general are considere somewhat valuable. Articles describing the reproductive system of a species outside of the class of mammalia are not relevant. Articles talking about human reproduction from other perspectives outside that of the biological sciences, such as from a historical or political perspective are not relevant.', '人类生殖系统', '我希望找到关于人类生殖系统的文章。', '', '人类生殖系统', '我正在寻找描述人类生殖系统的文章。', '从现代生物学家的角度用生物学术语描述人类生殖系统的文章被认为是相关的。描述整个系统的文章（例如组件的一般描述）或较低级别的描述（例如涉及系统早期发育或成年阶段维护的特定细胞或分子途径）是相关的。一般而言，对哺乳动物生殖系统的描述被认为具有一定的价值。描述哺乳动物纲以外物种的生殖系统的文章不相关。从生物科学之外的其他角度（例如从历史或政治角度）讨论人类生殖的文章是不相关的。', 'zh'),
            9: ExctractedCCNoReportQuery('10', 'Internet improve rural economy', 'How does Internet availability improve the rural economy?', 'Relevant articles should focus on the process of Internet availability contributing to the rural economy, but not on the result that what the government should do to utilize the Internet to improve the rural economy. Focusing on the influence on the city economy is also deemed irrelevant. Documents talking about how technology/ some specific components of the Internet  improves the rural economy/urbanization would be considered somewhat valuable.', '互联网 提升 乡村的 经济', '互联网普及如何提升乡村经济？', '', '互联网带动农村经济', '互联网的普及如何改善农村经济？', '相关文章应该关注互联网对农村经济的贡献过程，而不是政府应该如何利用互联网来改善农村经济的结果。关注对城市经济的影响也被认为是无关紧要的。讨论技术/互联网的某些特定组成部分如何改善农村经济/城市化的文件将被认为是有价值的。', 'zh'),
            40: ExctractedCCNoReportQuery('41', 'Pure gold nanoparticle production', 'I am looking for articles discussing the production of pure gold nanoparticles.', 'Only articles related to the production process of pure gold nano particles are considered relevant. Documents discussing the production of nanoparticles made off materials other than pure gold are not considered relevant. Additionally, documents focusing on the property or applications of the pure gold nano particles rather than their production are not considered relevant.', '纯金纳米颗粒的制备', '与纯金纳米颗粒的制作工艺相关的文章。', '', '纯金纳米颗粒生产', '我正在寻找讨论纯金纳米颗粒生产的文章。', '只有与纯金纳米粒子的生产过程相关的文章才被认为是相关的。讨论由纯金以外的材料制成的纳米颗粒的生产的文件被认为不相关。此外，关注纯金纳米颗粒的特性或应用而不是其生产的文件被认为不相关。', 'zh'),
        })

    def test_qrels(self):
        self._test_qrels('csl/trec-2023', count=11291, items={
            0: TrecQrel('1', 'csl-017102', 0, '0'),
            9: TrecQrel('1', 'csl-213347', 0, '0'),
            11290: TrecQrel('41', 'csl-166443', 0, '0'),
        })


if __name__ == '__main__':
    unittest.main()
