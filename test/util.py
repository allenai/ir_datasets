import unittest
import ir_datasets


class TestUtil(unittest.TestCase):
    def test_apply_sub_slice(self):
    	ass = ir_datasets.util.apply_sub_slice
    	self.assertEqual(ass(slice(0, 100), slice(0, 10)), slice(0, 10))
    	self.assertEqual(ass(slice(0, 10), slice(0, 100)), slice(0, 10))
    	self.assertEqual(ass(slice(10, 100), slice(0, 10)), slice(10, 20))
    	self.assertEqual(ass(slice(0, 10), slice(10, 100)), slice(10, 10))
    	self.assertEqual(ass(slice(0, 100), slice(0, None)), slice(0, 100))
    	self.assertEqual(ass(slice(0, 100), slice(0, -1)), slice(0, 99))
    	self.assertEqual(ass(slice(0, 100), slice(0, -2)), slice(0, 98))
    	self.assertEqual(ass(slice(0, 100), slice(1, -2)), slice(1, 98))
    	self.assertEqual(ass(slice(0, 100), slice(-1, None)), slice(99, 100))
    	self.assertEqual(ass(slice(0, 100), slice(-2, None)), slice(98, 100))
    	self.assertEqual(ass(slice(0, 100), slice(-2, -1)), slice(98, 99))
    	self.assertEqual(ass(slice(0, 100), slice(0/3, 1/3)), slice(0, 33))
    	self.assertEqual(ass(slice(0, 100), slice(1/3, 2/3)), slice(33, 66))
    	self.assertEqual(ass(slice(0, 100), slice(2/3, 3/3)), slice(66, 100))

    def test_corpus_id(self):
        # typical
        self.assertEqual(ir_datasets.corpus_id("msmarco-document/trec-dl-2019/judged"), "msmarco-document")
        # identity
        self.assertEqual(ir_datasets.corpus_id("msmarco-document"), "msmarco-document")
        # wikir doesn't support docs
        self.assertEqual(ir_datasets.corpus_id("wikir/en1k/test"), "wikir/en1k")
        # clueweb09 supports docs, but clueweb09/catb is a different subset
        self.assertEqual(ir_datasets.corpus_id("clueweb09/catb/trec-web-2009"), "clueweb09/catb")
        self.assertEqual(ir_datasets.corpus_id("clueweb09/catb"), "clueweb09/catb")
        self.assertEqual(ir_datasets.corpus_id("clueweb09"), "clueweb09")
        # clirmatrix uses matching patterns
        self.assertEqual(ir_datasets.corpus_id("clirmatrix/en"), "clirmatrix/en")
        self.assertEqual(ir_datasets.corpus_id("clirmatrix/en/bi139-full/de/train"), "clirmatrix/en")

    def test_html_find_charset(self):
        self.assertEqual(ir_datasets.util.html_parsing.find_charset(b'<head>\n  <title>Resultados Copa Universidad de Chile | FECH</title>\n  <meta http-equiv="Content-Type" content="text/html" />\n<style type="text/css" media="all">@import "/misc/drupal.css";</style>\n<script type="text/javascript"><!--\n  var BASE_URL = "/";'), None)
        self.assertEqual(ir_datasets.util.html_parsing.find_charset('<head>\n  <title>Resultados Copa Universidad de Chile | FECH</title>\n  <meta http-equiv="Content-Type" content="text/html" />\n<style type="text/css" media="all">@import "/misc/drupal.css";</style>\n<script type="text/javascript"><!--\n  var BASE_URL = "/";'), None)
        self.assertEqual(ir_datasets.util.html_parsing.find_charset(b'<head>\n  <title>Resultados Copa Universidad de Chile | FECH</title>\n  <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n<style type="text/css" media="all">@import "/misc/drupal.css";</style>\n<script type="text/javascript"><!--\n  var BASE_URL = "/";'), 'utf-8')
        self.assertEqual(ir_datasets.util.html_parsing.find_charset('<head>\n  <title>Resultados Copa Universidad de Chile | FECH</title>\n  <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n<style type="text/css" media="all">@import "/misc/drupal.css";</style>\n<script type="text/javascript"><!--\n  var BASE_URL = "/";'), 'utf-8')
        self.assertEqual(ir_datasets.util.html_parsing.find_charset(b'<html dir="ltr" lang="pt">\n<head>\n  <meta http-equiv="Content-Type" content="text/html; charset="ISO-8859-1" />\n   <meta name="keywords" content="Como se fosse um pai !!!!!, forum,sexo,portugal,banheira,snb,sexo gratis" />\n   <meta name="description" content="[Mapa] Como se fosse um pai !!!!! Piadas" />'), 'ISO-8859-1')
        self.assertEqual(ir_datasets.util.html_parsing.find_charset('<html dir="ltr" lang="pt">\n<head>\n  <meta http-equiv="Content-Type" content="text/html; charset="ISO-8859-1" />\n   <meta name="keywords" content="Como se fosse um pai !!!!!, forum,sexo,portugal,banheira,snb,sexo gratis" />\n   <meta name="description" content="[Mapa] Como se fosse um pai !!!!! Piadas" />'), 'ISO-8859-1')
        self.assertEqual(ir_datasets.util.html_parsing.find_charset(b'HTTP/1.1 200 OK\nContent-Type: text/html; charset=utf-8\nDate: Thu, 26 Feb 2009 23:08:09 GMT\nCache-Control: no-store, no-cache, must-revalidate, post-check=0, pre-check=0\nPragma: no-cache\nServer: Apache/2.2.3 (CentOS)\nConnection: close\nSet-Cookie: sid=l4qj4sq8haosicb4s0ilspmdc6; path=/\nExpires: Thu, 19 Nov 1981 08:52:00 GMT\nContent-Length: 33974'), 'utf-8')
        self.assertEqual(ir_datasets.util.html_parsing.find_charset('HTTP/1.1 200 OK\nContent-Type: text/html; charset=utf-8\nDate: Thu, 26 Feb 2009 23:08:09 GMT\nCache-Control: no-store, no-cache, must-revalidate, post-check=0, pre-check=0\nPragma: no-cache\nServer: Apache/2.2.3 (CentOS)\nConnection: close\nSet-Cookie: sid=l4qj4sq8haosicb4s0ilspmdc6; path=/\nExpires: Thu, 19 Nov 1981 08:52:00 GMT\nContent-Length: 33974'), 'utf-8')

    def test_decode_html(self):
        self.assertEqual(ir_datasets.util.html_parsing.decode_html(b'<meta charset="utf-8"/>\xc2\xa3'), '<meta charset="utf-8"/>£')
        self.assertEqual(ir_datasets.util.html_parsing.decode_html(b'<meta charset="iso8859-1"/>\xa3'), '<meta charset="iso8859-1"/>£')

    def test_sax_html_parser(self):
        self.assertEqual(ir_datasets.util.html_parsing.sax_html_parser(b'<meta charset="utf-8"/>\xc2\xa3'), ('', '£'))
        self.assertEqual(ir_datasets.util.html_parsing.sax_html_parser(b'<meta charset="iso8859-1"/>\xa3'), ('', '£'))
        self.assertEqual(ir_datasets.util.html_parsing.sax_html_parser(b'<title>Some <span>text</span></title>\n<body><script>this is all discarded <div></script><style a="b">so is this</style><div><span>other </span>  \xc2\xa3<span>stuff</span>!</div>   \n\n\r\ntext&gt;&#62;&#x3E;</body>'), ('Some text', 'other £stuff!\ntext>>>'))


if __name__ == '__main__':
    unittest.main()
