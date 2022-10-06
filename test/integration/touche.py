from unittest import main
from re import compile

from ir_datasets.formats import ToucheQuery, TrecQrel, ToucheTitleQuery, \
    ToucheComparativeQuery, ToucheQualityQrel, ToucheQualityCoherenceQrel, \
    ToucheQualityComparativeStanceQrel, ToucheComparativeStance, \
    ToucheControversialStanceQrel, ToucheControversialStance
from test.integration.base import DatasetIntegrationTest


class TestTouche(DatasetIntegrationTest):

    # noinspection PyTypeChecker
    def test_queries(self):
        self._test_queries(
            "argsme/2020-04-01/touche-2020-task-1",
            count=49,
            items={
                0: ToucheQuery(
                    query_id="1",
                    title="Should teachers get tenure?",
                    description=compile("A user has heard that some countries do give teach.{159}teachers vs. university professors is of interest\."),
                    narrative=compile("Highly relevant arguments make a clear statement a.{181}the situation of teachers' financial independence\."),
                ),
                48: ToucheQuery(
                    query_id="50",
                    title="Should everyone get a universal basic income?",
                    description=compile("Redistribution of wealth is a fundamental concept .{93}ver, a user wonders whether this truly would help\."),
                    narrative=compile("Highly relevant arguments take a clear stance towa.{134}mentioning universal basic income only in passing\."),
                ),
            }
        )
        self._test_queries(
            "argsme/1.0/touche-2020-task-1/uncorrected",
            count=49,
            items={
                0: ToucheQuery(
                    query_id="1",
                    title="Should teachers get tenure?",
                    description=compile("A user has heard that some countries do give teach.{159}teachers vs. university professors is of interest\."),
                    narrative=compile("Highly relevant arguments make a clear statement a.{181}the situation of teachers' financial independence\."),
                ),
                48: ToucheQuery(
                    query_id="50",
                    title="Should everyone get a universal basic income?",
                    description=compile("Redistribution of wealth is a fundamental concept .{93}ver, a user wonders whether this truly would help\."),
                    narrative=compile("Highly relevant arguments take a clear stance towa.{134}mentioning universal basic income only in passing\."),
                ),
            }
        )
        self._test_queries(
            "argsme/2020-04-01/touche-2020-task-1/uncorrected",
            count=49,
            items={
                0: ToucheQuery(
                    query_id="1",
                    title="Should teachers get tenure?",
                    description=compile("A user has heard that some countries do give teach.{159}teachers vs. university professors is of interest\."),
                    narrative=compile("Highly relevant arguments make a clear statement a.{181}the situation of teachers' financial independence\."),
                ),
                48: ToucheQuery(
                    query_id="50",
                    title="Should everyone get a universal basic income?",
                    description=compile("Redistribution of wealth is a fundamental concept .{93}ver, a user wonders whether this truly would help\."),
                    narrative=compile("Highly relevant arguments take a clear stance towa.{134}mentioning universal basic income only in passing\."),
                ),
            }
        )
        self._test_queries(
            "clueweb12/touche-2020-task-2",
            count=50,
            items={
                0: ToucheQuery(
                    query_id="1",
                    title="What is the difference between sex and love?",
                    description=compile("A potentially younger user has heard people talk a.{147}ontrast, what characterizes a loving relationship\."),
                    narrative=compile("Relevant documents will contain some description o.{155}f what people are looking for in either direction\."),
                ),
                49: ToucheQuery(
                    query_id="50",
                    title="Whose salary is higher: basketball or soccer players?",
                    description=compile("A young married couple raises a 14-year old boy wh.{313}income to players in different parts of the world\."),
                    narrative=compile("Highly relevant documents provide information on a.{496}iptions of basketball and soccer are not relevant\."),
                ),
            }
        )
        self._test_queries(
            "argsme/2020-04-01/touche-2021-task-1",
            count=50,
            items={
                0: ToucheTitleQuery(
                    query_id="51",
                    title="Do we need sex education in schools?"
                ),
                49: ToucheTitleQuery(
                    query_id="100",
                    title="Do we need cash?"
                ),
            }
        )
        self._test_queries(
            "clueweb12/touche-2021-task-2",
            count=50,
            items={
                0: ToucheQuery(
                    query_id="51",
                    title="What is better at reducing fever in children, Ibuprofen or Aspirin?",
                    description=compile("Younger parents have their 8-year old child sick\. .{400}en and aspirin for reducing the fever in children\."),
                    narrative=compile("Relevant documents will describe ibuprofen, aspiri.{258} or ingredients of the medicines are not relevant\."),
                ),
                49: ToucheQuery(
                    query_id="100",
                    title="Should I learn Python or R for data analysis?",
                    description=compile("Wondering whether you should use Python or R for d.{318}ore useful, flexible, easy to learn and efficient\."),
                    narrative=compile("Relevant documents should compare two programming .{430}re not related to data analysis, are not relevant\."),
                ),
            }
        )
        self._test_queries(
            "argsme/2020-04-01/processed/touche-2022-task-1",
            count=50,
            items={
                0: ToucheQuery(
                    query_id="1",
                    title="Should teachers get tenure?",
                    description=compile("A user has heard .{215} professors is of interest."),
                    narrative=compile("Highly relevant arguments make a clear statement about tenure for teachers in schools or universities. Relevant arguments consider tenure more generally, not specifically for teachers, or, instead of talking about tenure, consider the situation of teachers' financial independence."),
                ),
                49: ToucheQuery(
                    query_id="50",
                    title="Should everyone get a universal basic income?",
                    description=compile("Redistribution of wealth is a fundamental concept of many economies and social systems. A key component might be a universal basic income, however, a user wonders whether this truly would help."),
                    narrative=compile("Highly relevant arguments take a clear stance toward the universal basic income, giving clear premises. Relevant arguments offer only emotional arguments, or talk about minimum wages, mentioning universal basic income only in passing."),
                ),
            }
        )
        self._test_queries(
            "clueweb12/touche-2022-task-2",
            count=50,
            items={
                0: ToucheComparativeQuery(
                    query_id="2",
                    title="Which is better, a laptop or a desktop?",
                    objects=("laptop", "desktop"),
                    description=compile("A user wants to buy .{272} gaming desktop PC."),
                    narrative=compile("Highly relevant documents will describe the major similarities and dissimilarities of laptops and desktops along with the respective advantages and disadvantages of specific usage scenarios. A comparison of the technical and architectural characteristics without personal opinion, recommendation, or pros/cons is not relevant."),
                ),
                49: ToucheComparativeQuery(
                    query_id="100",
                    title="Should I learn Python or R for data analysis?",
                    objects=("Python", "R"),
                    description=compile("Wondering whether you should use Python or R for data analysis\? It's hard to know whether to use Python or R for data analysis. And that's especially true if you're a newbie data analyst looking for the right language to start with. Users are looking for documents that help them decide which programming language, Python or R, is better suited for data analysis, is more useful, flexible, easy to learn and efficient."),
                    narrative=compile("Relevant documents should compare two programming languages for data analysis: Python and R. Highly relevant documents should compare the two objects in terms of efficiency, usefulness, easiness to learn and so on. Highly relevant documents would ideally compare most of the features. The documents that only describe one language, but provide insights on how it is good for data analysis are relevant. Arguments and opinions of using one of the two or both for other tasks that are not related to data analysis, are not relevant."),
                ),
            }
        )
        self._test_queries(
            "touche-image/2022-06-13/touche-2022-task-3",
            count=50,
            items={
                0: ToucheQuery(
                    query_id="1",
                    title="Should teachers get tenure?",
                    description=compile("A user has heard .{215} professors is of interest."),
                    narrative=compile("Highly relevant arguments make a clear statement about tenure for teachers in schools or universities. Relevant arguments consider tenure more generally, not specifically for teachers, or, instead of talking about tenure, consider the situation of teachers' financial independence."),
                ),
                49: ToucheQuery(
                    query_id="50",
                    title="Should everyone get a universal basic income?",
                    description=compile("Redistribution of wealth is a fundamental concept of many economies and social systems. A key component might be a universal basic income, however, a user wonders whether this truly would help."),
                    narrative=compile("Highly relevant arguments take a clear stance toward the universal basic income, giving clear premises. Relevant arguments offer only emotional arguments, or talk about minimum wages, mentioning universal basic income only in passing."),
                ),
            }
        )

    # noinspection PyTypeChecker
    def test_qrels(self):
        self._test_qrels(
            "argsme/2020-04-01/touche-2020-task-1",
            count=2298,
            items={
                0: TrecQrel(
                    query_id="1",
                    doc_id="S197beaca-A971412e6",
                    relevance=0,
                    iteration="0"
                ),
                2297: TrecQrel(
                    query_id="50",
                    doc_id="Sffdf2e2e-A307df259",
                    relevance=2,
                    iteration="0"
                ),
            }
        )
        self._test_qrels(
            "argsme/1.0/touche-2020-task-1/uncorrected",
            count=2964,
            items={
                0: TrecQrel(
                    query_id="1",
                    doc_id="197beaca-2019-04-18T11:28:59Z-00001-000",
                    relevance=4,
                    iteration="0"
                ),
                2963: TrecQrel(
                    query_id="50",
                    doc_id="799d051-2019-04-18T11:47:02Z-00000-000",
                    relevance=-2,
                    iteration="Q0"
                ),
            }
        )
        self._test_qrels(
            "argsme/2020-04-01/touche-2020-task-1/uncorrected",
            count=2298,
            items={
                0: TrecQrel(
                    query_id="1",
                    doc_id="S21dc5a14-A8b896cb0",
                    relevance=4,
                    iteration="0"
                ),
                2297: TrecQrel(
                    query_id="50",
                    doc_id="Sffdf2e2e-A307df259",
                    relevance=2,
                    iteration="0"
                ),
            }
        )
        self._test_qrels(
            "clueweb12/touche-2020-task-2",
            count=1783,
            items={
                0: TrecQrel(
                    query_id="1",
                    doc_id="clueweb12-0001wb-05-12311",
                    relevance=0,
                    iteration="0"
                ),
                1782: TrecQrel(
                    query_id="50",
                    doc_id="clueweb12-0206wb-00-16297",
                    relevance=0,
                    iteration="0"
                ),
            }
        )
        self._test_qrels(
            "argsme/2020-04-01/touche-2021-task-1",
            count=3711,
            items={
                0: ToucheQualityQrel(
                    query_id="94",
                    doc_id="S522c7c3b-A8a87130b",
                    relevance=2,
                    quality=2,
                    iteration="0"
                ),
                3710: ToucheQualityQrel(
                    query_id="91",
                    doc_id="Sf0770da-A760eca8e",
                    relevance=0,
                    quality=1,
                    iteration="0"
                ),
            }
        )
        self._test_qrels(
            "clueweb12/touche-2021-task-2",
            count=2076,
            items={
                0: ToucheQualityQrel(
                    query_id="54",
                    doc_id="clueweb12-0205wb-64-11095",
                    relevance=0,
                    quality=0,
                    iteration="0"
                ),
                2075: ToucheQualityQrel(
                    query_id="86",
                    doc_id="clueweb12-0008wb-85-29079",
                    relevance=0,
                    quality=0,
                    iteration="0"
                ),
            }
        )
        self._test_qrels(
            "argsme/2020-04-01/processed/touche-2022-task-1",
            count=6841,
            items={
                0: ToucheQualityCoherenceQrel(
                    query_id="1",
                    doc_id="Sc065954f-Ae72bc9c6__PREMISE__41,Sc065954f-Ae72bc9c6__CONC__1",
                    relevance=2,
                    quality=2,
                    coherence=2,
                    iteration="0"
                ),
                6840: ToucheQualityCoherenceQrel(
                    query_id="50",
                    doc_id="Sffdf2e2e-A20e9dd06__PREMISE__4,Sffdf2e2e-A20e9dd06__PREMISE__5",
                    relevance=1,
                    quality=1,
                    coherence=1,
                    iteration="0"
                ),
            }
        )
        self._test_qrels(
            "clueweb12/touche-2022-task-2",
            count=2107,
            items={
                0: ToucheQualityComparativeStanceQrel(
                    query_id="12",
                    doc_id="clueweb12-0002wb-18-34442___2",
                    relevance=0,
                    quality=2,
                    stance=ToucheComparativeStance.NO,
                    iteration="0"
                ),
                2106: ToucheQualityComparativeStanceQrel(
                    query_id="70",
                    doc_id="clueweb12-1900tw-42-07368___7",
                    relevance=1,
                    quality=1,
                    stance=ToucheComparativeStance.NO,
                    iteration="0"
                ),
            }
        )
        self._test_qrels(
            "touche-image/2022-06-13/touche-2022-task-3",
            count=19821,
            items={
                0: ToucheControversialStanceQrel(
                    query_id="1",
                    doc_id="Ib7fc7d5f8ee59d62",
                    relevance=1,
                    stance=ToucheControversialStance.ONTOPIC,
                ),
                19820: ToucheControversialStanceQrel(
                    query_id="50",
                    doc_id="I490ab3908d308757",
                    relevance=1,
                    stance=ToucheControversialStance.CON,
                ),
            }
        )


if __name__ == "__main__":
    main()
