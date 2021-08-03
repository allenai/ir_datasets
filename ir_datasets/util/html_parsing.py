import io
import chardet
import ir_datasets


def sax_html_parser(body):
    etree = ir_datasets.lazy_libs.lxml_html().etree
    sax = SaxExtractor()
    parser = etree.HTMLParser(target=sax)
    if isinstance(body, bytes):
        encoding = chardet.detect(body)['encoding'] or 'utf8'
        cdc = codecs.lookup(encoding)
        while body:
            text, count = cdc.decode(body, 'ignore')
            parser.feed(text)
            body = body[count:]
    else:
        parser.feed(body)
    parser.close()
    return str(sax)

class SaxExtractor:
    IGNORE_TAGS = {'noscript', 'meta', 'input', 'script', 'style'}
    def __init__(self):
        self.text = io.StringIO()
        self.ignore_tag_stack = []
    def __str__(self):
        self.text.seek(0)
        return self.text.read()
    def data(self, data):
        if not self.ignore_tag_stack:
            self.text.write(data)
    def start(self, tag, attrs):
        tag = tag.lower()
        if tag in self.IGNORE_TAGS:
            self.ignore_tag_stack.append(tag)
    def end(self, tag):
        tag = tag.lower()
        if tag in self.IGNORE_TAGS:
            while self.ignore_tag_stack and self.ignore_tag_stack.pop() != tag:
                pass
    def close(self):
        pass
    def comment(self, data):
        pass
    def doctype(self, *args):
        pass
    def pi(self, *args):
        pass
