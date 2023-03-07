import re
import unittest
from ir_datasets.formats import TrecQrel, GenericQuery, GenericDoc, GenericDocPair
from .base import DatasetIntegrationTest


class TestNeuMarco(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('neumarco/fa', count=8841823, items={
            0: GenericDoc('0', re.compile('^ حضور ارتباطات در میان ذهن \u200c های علمی برای موفقیت پروژه منهتن به همان اندازه مهم بود که هوش علمی بود.{57}می معلق است ، چیزی است که موفقیت آن \u200c ها به معنای واقعی آن است ؛ صدها هزار زندگی بی \u200c گناه نابود شد\\.$', flags=48)),
            9: GenericDoc('9', ' یکی از دلایل اصلی انتخاب هنفورد به عنوان یک مکان برای پروژه منهتن رآکتور B نزدیکی به رودخانه کلمبیا بود ، بزرگترین رودخانه \u200c ای که از ساحل آمریکای شمالی به اقیانوس آرام می \u200c ریزد.'),
            8841822: GenericDoc('8841822', re.compile('^ تصویر کامل اندازه را مشاهده کنید\\. پشت صحنه نور خیره کننده نشان می دهد که تماشاگران اووه و آه در چها.{272}ده نمک \u200c های فلزی و اکسید فلزی وجود دارد که برای تولید مجموعه \u200c ای از رنگ \u200c ها واکنش نشان می \u200c دهند\\.$', flags=48)),
        })
        self._test_docs('neumarco/ru', count=8841823, items={
            0: GenericDoc('0', re.compile('^ Присутствие общения среди научных умов было не менее важным для успеха Манхэттенского проекта, как .{98}лей и инженеров, это то, что их успех действительно означал; сотни тысяч невинных жизней уничтожены\\.$', flags=48)),
            9: GenericDoc('9', re.compile('^ Одной из главных причин, по которой Хэнфорд был выбран в качестве объекта для « B Reactor » Манхэтт.{31}сть к реке Колумбия, самой большой реке, протекающей в Тихий океан с северо американского побережья\\.$', flags=48)),
            8841822: GenericDoc('8841822', re.compile('^ Просмотр полноразмерного изображения\\. За кулисами ослепительного света видно, что зрители ooh и ahh.{313}ми, в основном солями металлов и оксидами металлов, которые реагируют на получение множества цветов\\.$', flags=48)),
        })
        self._test_docs('neumarco/zh', count=8841823, items={
            0: GenericDoc('0', ' 在科学头脑中的交流对曼哈顿项目的成功同样重要，因为科学智慧是科学智慧。 原子研究人员和工程师令人印象深刻的成就中唯一的云就是他们的成功真正意味着什么；数十万无辜的生命被消灭了。'),
            9: GenericDoc('9', ' 汉福德被选定为曼哈顿项目B反应堆的一个主要原因是它靠近哥伦比亚河，这是北美海岸流入太平洋的最大河流。'),
            8841822: GenericDoc('8841822', ' 查看全尺寸图像。 在耀眼的灯光的背后，7月4日的观众们都是精心制作的烟花。 不管是红色、白色和蓝色的喷泉还是紫色的火花，每个烟花都充满了正确的化学物质组合，以创造这些五颜六色的灯光。 在每一个手工烟花中，都有少量的特殊化学物质，主要是金属盐和金属氧化物，它们会反应产生一系列的颜色。'),
        })

    def test_queries(self):
        for lang in ['fa', 'ru', 'zh']:
            self._test_queries(f'neumarco/{lang}/train', count=808731, items={
                0: GenericQuery('121352', 'define extreme'),
                9: GenericQuery('492875', 'sanitizer temperature'),
                808730: GenericQuery('50393', 'benefits of boiling lemons and drinking juice.')
            })
            self._test_queries(f'neumarco/{lang}/train/judged', count=502939, items={
                0: GenericQuery('121352', 'define extreme'),
                9: GenericQuery('54528', 'blood clots in urine after menopause'),
                502938: GenericQuery('50393', 'benefits of boiling lemons and drinking juice.')
            })
            self._test_queries(f'neumarco/{lang}/dev', count=101093, items={
                0: GenericQuery('1048578', 'cost of endless pools/swim spa'),
                9: GenericQuery('1048587', 'what is patron'),
                101092: GenericQuery('524285', 'treadmill incline meaning')
            })
            self._test_queries(f'neumarco/{lang}/dev/small', count=6980, items={
                0: GenericQuery('1048585', "what is paula deen's brother"),
                9: GenericQuery('524699', 'tricare service number'),
                6979: GenericQuery('1048565', 'who plays sebastian michaelis'),
            })
            self._test_queries(f'neumarco/{lang}/dev/judged', count=55578, items={
                0: GenericQuery('1048578', 'cost of endless pools/swim spa'),
                9: GenericQuery('1048601', 'what is pastoral medicine'),
                55577: GenericQuery('1048570', 'what is pearls before swine?')
            })

    def test_qrels(self):
        for lang in ['fa', 'ru', 'zh']:
            self._test_qrels(f'neumarco/{lang}/train', count=532761, items={
                0: TrecQrel('1185869', '0', 1, '0'),
                9: TrecQrel('186154', '1160', 1, '0'),
                532760: TrecQrel('405466', '8841735', 1, '0')
            })
            self._test_qrels(f'neumarco/{lang}/train/judged', count=532761, items={
                0: TrecQrel('1185869', '0', 1, '0'),
                9: TrecQrel('186154', '1160', 1, '0'),
                532760: TrecQrel('405466', '8841735', 1, '0')
            })
            self._test_qrels(f'neumarco/{lang}/dev', count=59273, items={
                0: TrecQrel('1102432', '2026790', 1, '0'),
                9: TrecQrel('300674', '7067032', 1, '0'),
                59272: TrecQrel('371455', '8009476', 1, '0')
            })
            self._test_qrels(f'neumarco/{lang}/dev/small', count=7437, items={
                0: TrecQrel('300674', '7067032', 1, '0'),
                9: TrecQrel('54544', '7068203', 1, '0'),
                7436: TrecQrel('195199', '8009377', 1, '0'),
            })
            self._test_qrels(f'neumarco/{lang}/dev/judged', count=59273, items={
                0: TrecQrel('1102432', '2026790', 1, '0'),
                9: TrecQrel('300674', '7067032', 1, '0'),
                59272: TrecQrel('371455', '8009476', 1, '0')
            })

    def test_docpairs(self):
        for lang in ['fa', 'ru', 'zh']:
            self._test_docpairs(f'neumarco/{lang}/train', count=269919004, items={
                0: GenericDocPair('662731', '193249', '2975302'),
                9: GenericDocPair('411362', '31018', '4238671'),
                269919003: GenericDocPair('88228', '5117891', '7075853')
            })



if __name__ == '__main__':
    unittest.main()
