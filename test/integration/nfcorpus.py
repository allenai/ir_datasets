import re
import unittest
import ir_datasets
from ir_datasets.datasets.nfcorpus import NfCorpusDoc, NfCorpusQuery, NfCorpusVideoQuery
from ir_datasets.formats import TrecQrel, GenericQuery
from .base import DatasetIntegrationTest


_logger = ir_datasets.log.easy()


class TestNf(DatasetIntegrationTest):
    def test_nf_docs(self):
        self._test_docs('nfcorpus', count=5371, items={
            0: NfCorpusDoc('MED-1', 'http://www.ncbi.nlm.nih.gov/pubmed/23092936', 'Birth Weight, Head Circumference, and Prenatal Exposure to Acrylamide from Maternal Diet: The European Prospective Mother–Child Study (NewGeneris)', re.compile('^Abstract Background: Acrylamide is a common dietary exposure that crosses the human placenta\\. It is .{1582}ed, these findings suggest that dietary intake of acrylamide should be reduced among pregnant women\\.$', flags=48)),
            9: NfCorpusDoc('MED-10', 'http://www.ncbi.nlm.nih.gov/pubmed/25329299', 'Statin Use and Breast Cancer Survival: A Nationwide Cohort Study from Finland', re.compile('^Abstract Recent studies have suggested that statins, an established drug group in the prevention of .{1533}evaluated further in a clinical trial testing statins’ effect on survival in breast cancer patients\\.$', flags=48)),
            5370: NfCorpusDoc('MED-5371', 'http://www.ncbi.nlm.nih.gov/pubmed/21931319', 'Omega-3 Fatty Acids for the Treatment of Depression: Systematic Review and Meta-Analysis', re.compile('^Abstract We conducted a meta\\-analysis of randomized, placebo\\-controlled trials of omega\\-3 fatty acid.{1604}the treatment efficacy observed in the published literature may be attributable to publication bias\\.$', flags=48)),
        })

    def test_nf_queries(self):
        self._test_queries('nfcorpus/train', count=2594, items={
            0: NfCorpusQuery('PLAIN-10', 'how contaminated are our children ?', re.compile("^how contaminated are our children \\? in a study .*health , tuna , turkey , vegans , vegetarians , women 's health - -$", flags=48)),
            9: NfCorpusQuery('PLAIN-1010', 'deli meat', "deli meat - - processed meat , meat , beef , eggs , pork , plant-based diets , chicken , diabetes , turkey , poultry , animal products , women 's health , processed foods , nitrosamines , vegans - -"),
            2593: NfCorpusQuery('PLAIN-999', 'dandelion', 'dandelion - - phytonutrients , peppermint , lemongrass , lemon verbena , red tea , rooibos , thyme , tea , rosemary , rose hips , lavender , korea , chamomile tea , beverages , bergamots - -'),
        })
        self._test_queries('nfcorpus/train/nontopic', count=1141, items={
            0: GenericQuery('PLAIN-10', 'how contaminated are our children ?'),
            9: GenericQuery('PLAIN-110', 'how to get our kids to eat their vegetables'),
            1140: GenericQuery('PLAIN-99', 'quadrupling breast cancer survival'),
        })
        self._test_queries('nfcorpus/train/video', count=812, items={
            0: NfCorpusVideoQuery('PLAIN-2427', 'heart of gold : turmeric vs. exercise', 'diet and exercise synergize to improve endothelial function , the ability of our arteries to relax normally .'),
            9: NfCorpusVideoQuery('PLAIN-2438', 'what causes diabetes ?', 'saturated fat can be toxic to the insulin-producing beta cells in the pancreas , explaining why animal fat consumption can impair insulin secretion , not just insulin sensitivity .'),
            811: NfCorpusVideoQuery('PLAIN-3474', 'fish consumption and suicide', 'the mercury content in fish may help explain links found between fish intake and mental disorders , depression , and suicide .'),
        })
        self._test_queries('nfcorpus/dev', count=325, items={
            0: NfCorpusQuery('PLAIN-1', 'why deep fried foods may cause cancer', re.compile("^why deep fried foods may cause cancer in the .*, throat cancer , turkey , vitamin c , women 's health - -$", flags=48)),
            9: NfCorpusQuery('PLAIN-1087', 'easter island', 'easter island - - mortality , muscle strength , morbidity , mood , mitochondria , oxidative stress , rapamycin , wound healing , tor , sexual health , reproductive health , longevity , lifespan , caloric restriction , calories - -'),
            324: NfCorpusQuery('PLAIN-996', 'cytoskeleton', "cytoskeleton - - natural toxins , nutrition myths , monounsaturated fats , metastases , guacamole , insecticides , nuts , oral cancer , taxol , women 's health , phytosterols , persin , paclitaxel , fungicides , fda - -"),
        })
        self._test_queries('nfcorpus/dev/nontopic', count=144, items={
            0: GenericQuery('PLAIN-1', 'why deep fried foods may cause cancer'),
            9: GenericQuery('PLAIN-174', 'cinnamon for diabetes'),
            143: GenericQuery('PLAIN-90', 'how to boost the benefits of exercise'),
        })
        self._test_queries('nfcorpus/dev/video', count=102, items={
            0: NfCorpusVideoQuery('PLAIN-2429', 'diverticulosis : when our most common gut disorder hardly existed', 'more than two-thirds of americans over age 60 have diverticulosis , but it was nearly unknown a century ago and remained extremely rare among populations eating whole food plant-based diets .'),
            9: NfCorpusVideoQuery('PLAIN-2519', 'how much added sugar is too much ?', 'are table sugar and high fructose corn syrup just empty calories or can they be actively harmful ?'),
            101: NfCorpusVideoQuery('PLAIN-3471', 'uprooting the leading causes of death', 'death in america is largely a foodborne illness . focusing on studies published just over the last year in peer-reviewed scientific medical journals , dr. greger offers practical advice on how best to feed ourselves and our families to prevent , treat , and even reverse many of the top 15 killers in the united states .'),
        })
        self._test_queries('nfcorpus/test', count=325, items={
            0: NfCorpusQuery('PLAIN-1008', 'deafness', 'deafness - - industrial toxins , infants , lead , medications , india , in vitro studies , haritaki fruit , heavy metals , herbs , mercury , mortality , phytonutrients , side effects , supplements , triphala - -'),
            9: NfCorpusQuery('PLAIN-1098', 'eggnog', 'eggnog - - nutmeg , safety limits , spices , mood , miscarriage , cost savings , amphetamines - -'),
            324: NfCorpusQuery('PLAIN-997', 'czechoslovakia', "czechoslovakia - - body odor , men 's health , spain , singapore , omnivores , physical attraction , vegetarians , sexual health , plant-based diets , persistent organic pollutants , new york city , flame-retardant chemicals , fibroids , china , california - -"),
        })
        self._test_queries('nfcorpus/test/nontopic', count=144, items={
            0: GenericQuery('PLAIN-102', 'stopping heart disease in childhood'),
            9: GenericQuery('PLAIN-186', 'best treatment for constipation'),
            143: GenericQuery('PLAIN-91', 'chronic headaches and pork parasites'),
        })
        self._test_queries('nfcorpus/test/video', count=102, items={
            0: NfCorpusVideoQuery('PLAIN-2430', 'preventing brain loss with b vitamins ?', 'one week on a plant-based diet can significantly drop blood levels of homocysteine , a toxin associated with cognitive decline and alzheimer ’ s disease . without vitamin b12 supplementation , though , a long-term plant-based diet could make things worse .'),
            9: NfCorpusVideoQuery('PLAIN-2520', 'caloric restriction vs. plant-based diets', 'what is the best strategy to lower the level of the cancer-promoting growth hormone igf-1 ?'),
            101: NfCorpusVideoQuery('PLAIN-3472', 'how doctors responded to being named a leading killer', 'what was the medical community ’ s reaction to being named the third leading cause of death in the united states ?'),
        })

    def test_gov2_qrels(self):
        self._test_qrels('nfcorpus/train', count=139350, items={
            0: TrecQrel('PLAIN-3', 'MED-2436', 3, '0'),
            9: TrecQrel('PLAIN-3', 'MED-2431', 2, '0'),
            139349: TrecQrel('PLAIN-3474', 'MED-4634', 2, '0'),
        })
        self._test_qrels('nfcorpus/train/nontopic', count=37383, items={
            0: TrecQrel('PLAIN-3', 'MED-2436', 3, '0'),
            9: TrecQrel('PLAIN-3', 'MED-2431', 2, '0'),
            37382: TrecQrel('PLAIN-3474', 'MED-4634', 2, '0'),
        })
        self._test_qrels('nfcorpus/train/video', count=27465, items={
            0: TrecQrel('PLAIN-2427', 'MED-1507', 3, '0'),
            9: TrecQrel('PLAIN-2427', 'MED-1637', 2, '0'),
            27464: TrecQrel('PLAIN-3474', 'MED-4634', 2, '0'),
        })
        self._test_qrels('nfcorpus/dev', count=14589, items={
            0: TrecQrel('PLAIN-1', 'MED-2421', 3, '0'),
            9: TrecQrel('PLAIN-1', 'MED-4070', 2, '0'),
            14588: TrecQrel('PLAIN-3471', 'MED-5342', 3, '0'),
        })
        self._test_qrels('nfcorpus/dev/nontopic', count=4353, items={
            0: TrecQrel('PLAIN-1', 'MED-2421', 3, '0'),
            9: TrecQrel('PLAIN-1', 'MED-4070', 2, '0'),
            4352: TrecQrel('PLAIN-3471', 'MED-5342', 3, '0'),
        })
        self._test_qrels('nfcorpus/dev/video', count=3068, items={
            0: TrecQrel('PLAIN-2429', 'MED-974', 3, '0'),
            9: TrecQrel('PLAIN-2439', 'MED-5325', 2, '0'),
            3067: TrecQrel('PLAIN-3471', 'MED-5342', 3, '0'),
        })
        self._test_qrels('nfcorpus/test', count=15820, items={
            0: TrecQrel('PLAIN-2', 'MED-2427', 3, '0'),
            9: TrecQrel('PLAIN-2', 'MED-5324', 1, '0'),
            15819: TrecQrel('PLAIN-3472', 'MED-3627', 2, '0'),
        })
        self._test_qrels('nfcorpus/test/nontopic', count=4540, items={
            0: TrecQrel('PLAIN-2', 'MED-2427', 3, '0'),
            9: TrecQrel('PLAIN-2', 'MED-5324', 1, '0'),
            4539: TrecQrel('PLAIN-3472', 'MED-3627', 2, '0'),
        })
        self._test_qrels('nfcorpus/test/video', count=3108, items={
            0: TrecQrel('PLAIN-2430', 'MED-980', 3, '0'),
            9: TrecQrel('PLAIN-2430', 'MED-3137', 3, '0'),
            3107: TrecQrel('PLAIN-3472', 'MED-3627', 2, '0'),
        })


if __name__ == '__main__':
    unittest.main()
