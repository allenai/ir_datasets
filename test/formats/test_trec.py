import os
import shutil
import unittest
from ir_datasets.formats import TrecQrel, TrecQrels, TrecQuery, TrecQueries, TrecDoc, TrecDocs
from ir_datasets.util import StringFile


class TestTrec(unittest.TestCase):

    def test_qrels(self):
        mock_file = StringFile('''
Q0 0 D1 3
Q0 1 D2   2

Q0 0\tD3 3
Q0 1 D2 1
Q1 0 D2 1
'''.lstrip())
        QREL_DEFS = {}
        expected_results = [
            TrecQrel('Q0', 'D1', 3,  '0'),
            TrecQrel('Q0', 'D2', 2,  '1'),
            TrecQrel('Q0', 'D3', 3,  '0'),
            TrecQrel('Q0', 'D2', 1,  '1'),
            TrecQrel('Q1', 'D2', 1,  '0'),
        ]

        qrels = TrecQrels(mock_file, QREL_DEFS)
        self.assertEqual(qrels.qrels_path(), 'MOCK')
        self.assertEqual(qrels.qrels_defs(), QREL_DEFS)
        self.assertEqual(list(qrels.qrels_iter()), expected_results)

    def test_qrels_bad_line(self):
        mock_file = StringFile('''
Q0 0 D1 3
Q0 1 D2   2

Q0 0\tD3 3
Q0 1 D2 1
BAD LINE
Q1 0 D2 1
'''.lstrip())

        QREL_DEFS = {}

        qrels = TrecQrels(mock_file, QREL_DEFS)
        with self.assertRaises(RuntimeError):
            list(qrels.qrels_iter())

    def test_queries(self):
        mock_file = StringFile('''
<top>

<num> Number: Q100A 
<title>    Topic: Some title

<desc>  Description:  
Descriptive text
split on multiple lines

<narr> Narrative: 
Further elaboration of the query intent
split on multiple lines

</top>

<top>

<num> 102 
<title> Query 2

<desc>
Q2 description

<narr> Narrative: 
Q2 narrative

</top>
'''.lstrip())
        expected_results = [
            TrecQuery('Q100A', 'Some title', "Descriptive text\nsplit on multiple lines",  'Further elaboration of the query intent\nsplit on multiple lines'),
            TrecQuery('102', 'Query 2', "Q2 description",  'Q2 narrative'),
        ]

        queries = TrecQueries(mock_file)
        self.assertEqual(queries.queries_path(), 'MOCK')
        self.assertEqual(list(queries.queries_iter()), expected_results)

    def test_docs(self):
        mock_file = StringFile('''
<DOC>
<DOCNO>  D100A   </DOCNO>
<PARENT> Something </PARENT>
<HT> Some text  </HT>

<HEADLINE>
<AU>   Header Text </AU>
Daily Report 

</HEADLINE>

<TEXT>
Main body text
on multiple lines

with <F P=102> some markup
</F> here. Also, some invalid <T> markup &amp;. 
</TEXT>

</DOC>

<DOC>
<DOCNO>  101   </DOCNO>

<TEXT>
More body text
</TEXT>

</DOC>
'''.lstrip())
        expected_results = [
            TrecDoc(doc_id='D100A', text='\n\n   Header Text \nDaily Report \n\n\n\nMain body text\non multiple lines\n\nwith  some markup\n here. Also, some invalid  markup &. \n\n', marked_up_doc='<HEADLINE>\n<AU>   Header Text </AU>\nDaily Report \n\n</HEADLINE>\n<TEXT>\nMain body text\non multiple lines\n\nwith <F P=102> some markup\n</F> here. Also, some invalid <T> markup &amp;. \n</TEXT>\n'),
            TrecDoc(doc_id='101', text='\n\nMore body text\n\n', marked_up_doc='<TEXT>\nMore body text\n</TEXT>\n'),
        ]

        docs = TrecDocs(mock_file)
        self.assertEqual(docs.docs_path(), 'MOCK')
        self.assertEqual(list(docs.docs_iter()), expected_results)

    def tearDown(self):
        if os.path.exists('MOCK.pklz4'):
            shutil.rmtree('MOCK.pklz4')

if __name__ == '__main__':
    unittest.main()
