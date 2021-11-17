from unittest import main

from ir_datasets.formats import ToucheQuery, TrecQrel, ToucheSimpleQuery
from test.integration.base import DatasetIntegrationTest


class TestTouche(DatasetIntegrationTest):

    def test_queries(self):
        self._test_queries(
            "touche/2020/task-1",
            count=49,
            items={
                0: ToucheQuery(
                    query_id='1',
                    title='Should teachers get tenure?',
                    description="A user has heard that some countries do give teachers tenure and others don't. Interested in the reasoning for or against tenure, the user searches for positive and negative arguments. The situation of school teachers vs. university professors is of interest.",
                    narrative="Highly relevant arguments make a clear statement about tenure for teachers in schools or universities. Relevant arguments consider tenure more generally, not specifically for teachers, or, instead of talking about tenure, consider the situation of teachers' financial independence."
                ),
                48: ToucheQuery(
                    query_id='50',
                    title='Should everyone get a universal basic income?',
                    description='Redistribution of wealth is a fundamental concept of many economies and social systems. A key component might be a universal basic income, however, a user wonders whether this truly would help.',
                    narrative='Highly relevant arguments take a clear stance toward the universal basic income, giving clear premises. Relevant arguments offer only emotional arguments, or talk about minimum wages, mentioning universal basic income only in passing.'
                ),
            }
        )
        self._test_queries(
            "touche/2020/task-2",
            count=50,
            items={
                0: ToucheQuery(
                    query_id='1',
                    title='What is the difference between sex and love?',
                    description='A potentially younger user has heard people talk about "good sex" only being possible in a loving relationship. They now want to know what people look for when just being interested in sex or, in contrast, what characterizes a loving relationship.',
                    narrative='Relevant documents will contain some description or explanation of what sex and/or love is. A highly relevant document will even contrast sex-only from loving relationships with some potential background of what people are looking for in either direction.'
                ),
                49: ToucheQuery(
                    query_id='50',
                    title='Whose salary is higher: basketball or soccer players?',
                    description="A young married couple raises a 14-year old boy who loves sports and is equally good at playing both basketball and soccer. Thinking about their kid's future, the parents wonder about the potential future income practicing either basketball or soccer. They are searching now for statistics to estimate which kind of sport, soccer or basketball, provides a higher income to players in different parts of the world.",
                    narrative='Highly relevant documents provide information on average and top earnings in both sports on different levels of professionalism (e.g., NBA vs. European basketball leagues, playing in the MLS or some first or second soccer league in Europe, Asia or Southern America, etc.). Relevant documents provide data on average and top incomes in either of the sports. Documents with information about sports stars and their salaries but without data on average incomes make it rather difficult to compare medium earnings are not relevant. Also general descriptions of basketball and soccer are not relevant.'
                ),
            }
        )
        self._test_queries(
            "touche/2021/task-1",
            count=50,
            items={
                0: ToucheSimpleQuery(
                    query_id='51',
                    title='Do we need sex education in schools?'
                ),
                49: ToucheSimpleQuery(
                    query_id='100',
                    title='Do we need cash?'
                ),
            }
        )
        self._test_queries(
            "touche/2021/task-2",
            count=50,
            items={
                0: ToucheQuery(
                    query_id='51',
                    title='What is better at reducing fever in children, Ibuprofen or Aspirin?',
                    description="Younger parents have their 8-year old child sick. They know that ibuprofen and aspirin can help to reduce fever, but they also heard that one of them is not recommended for children and elderly people. The best solution would be to seek for a doctor's advice, but before that, the parents want to find more information on the web. They search for credible documents from reliable health-related sources that compare the efficacy and safety of ibuprofen and aspirin for reducing the fever in children.",
                    narrative='Relevant documents will describe ibuprofen, aspirin and possibly other medicine that can help to reduce fever, especially in children. Highly relevant documents will specifically compare and conclude which of the two is more efficient and safer for children. Documents that only describe the chemical formula or ingredients of the medicines are not relevant.'
                ),
                49: ToucheQuery(
                    query_id='100',
                    title='Should I learn Python or R for data analysis?',
                    description="Wondering whether you should use Python or R for data analysis? It's hard to know whether to use Python or R for data analysis. And that's especially true if you're a newbie data analyst looking for the right language to start with. Users are looking for documents that help them decide which programming language, Python or R, is better suited for data analysis, is more useful, flexible, easy to learn and efficient.",
                    narrative='Relevant documents should compare two programming languages for data analysis: Python and R. Highly relevant documents should compare the two objects in terms of efficiency, usefulness, easiness to learn and so on. Highly relevant documents would ideally compare most of the features. The documents that only describe one language, but provide insights on how it is good for data analysis are relevant. Arguments and opinions of using one of the two or both for other tasks that are not related to data analysis, are not relevant.'
                ),
            }
        )

    def test_qrels(self):
        self._test_qrels(
            "touche/2020/task-1",
            count=2298,
            items={
                0: TrecQrel(
                    query_id='1',
                    doc_id='S197beaca-A971412e6',
                    relevance=0,
                    iteration='0'
                ),
                2297: TrecQrel(
                    query_id='50',
                    doc_id='Sffdf2e2e-A307df259',
                    relevance=2,
                    iteration='0'
                ),
            }
        )
        self._test_qrels(
            "touche/2020/task-1/argsme-1.0-uncorrected",
            count=2964,
            items={
                0: TrecQrel(
                    query_id='1',
                    doc_id='197beaca-2019-04-18T11:28:59Z-00001-000',
                    relevance=4,
                    iteration='0'
                ),
                2963: TrecQrel(
                    query_id='50',
                    doc_id='799d051-2019-04-18T11:47:02Z-00000-000',
                    relevance=-2,
                    iteration='Q0'
                ),
            }
        )
        self._test_qrels(
            "touche/2020/task-1/argsme-2020-04-01-uncorrected",
            count=2298,
            items={
                0: TrecQrel(
                    query_id='1',
                    doc_id='S21dc5a14-A8b896cb0',
                    relevance=4,
                    iteration='0'
                ),
                2297: TrecQrel(
                    query_id='50',
                    doc_id='Sffdf2e2e-A307df259',
                    relevance=2,
                    iteration='0'
                ),
            }
        )
        self._test_qrels(
            "touche/2020/task-2",
            count=1783,
            items={
                0: TrecQrel(
                    query_id='1',
                    doc_id='clueweb12-0001wb-05-12311',
                    relevance=0,
                    iteration='0'
                ),
                1782: TrecQrel(
                    query_id='50',
                    doc_id='clueweb12-0206wb-00-16297',
                    relevance=0,
                    iteration='0'
                ),
            }
        )
        self._test_qrels(
            "touche/2021/task-1/relevance",
            count=3711,
            items={
                0: TrecQrel(
                    query_id='94',
                    doc_id='S522c7c3b-A8a87130b',
                    relevance=2,
                    iteration='0'
                ),
                3710: TrecQrel(
                    query_id='91',
                    doc_id='Sf0770da-A760eca8e',
                    relevance=0,
                    iteration='0'
                ),
            }
        )
        self._test_qrels(
            "touche/2021/task-1/quality",
            count=3711,
            items={
                0: TrecQrel(
                    query_id='94',
                    doc_id='S522c7c3b-A8a87130b',
                    relevance=2,
                    iteration='0'
                ),
                3710: TrecQrel(
                    query_id='91',
                    doc_id='Sf0770da-A760eca8e',
                    relevance=1,
                    iteration='0'
                ),
            }
        )
        self._test_qrels(
            "touche/2021/task-2/relevance",
            count=2076,
            items={
                0: TrecQrel(
                    query_id='54',
                    doc_id='clueweb12-0205wb-64-11095',
                    relevance=0,
                    iteration='0'
                ),
                2075: TrecQrel(
                    query_id='86',
                    doc_id='clueweb12-0008wb-85-29079',
                    relevance=0,
                    iteration='0'
                ),
            }
        )
        self._test_qrels(
            "touche/2021/task-2/quality",
            count=2076,
            items={
                0: TrecQrel(
                    query_id='54',
                    doc_id='clueweb12-0205wb-64-11095',
                    relevance=0,
                    iteration='0'
                ),
                2075: TrecQrel(
                    query_id='86',
                    doc_id='clueweb12-0008wb-85-29079',
                    relevance=0,
                    iteration='0'
                ),
            }
        )


if __name__ == "__main__":
    main()
