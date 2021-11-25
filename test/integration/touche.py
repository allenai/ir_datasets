from unittest import main
from re import compile

from ir_datasets.formats import ToucheQuery, TrecQrel, ToucheTitleQuery
from ir_datasets.formats.touche import ToucheQualityQrel
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


if __name__ == "__main__":
    main()
