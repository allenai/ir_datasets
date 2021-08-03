import io
import codecs
import ir_datasets


def textify_sax(body, encoding=None):
    etree = ir_datasets.lazy_libs.lxml_etree()
    sax = SaxTextifier()
    parser = etree.HTMLParser(target=sax)
    if isinstance(body, bytes):
        if encoding is None:
            chardet = ir_datasets.lazy_libs.chardet()
            encoding = chardet.detect(body)['encoding'] or 'utf8'
        cdc = codecs.lookup(encoding)
        while body:
            text, count = cdc.decode(body, 'ignore')
            parser.feed(text)
            body = body[count:]
    elif isinstance(body, str):
        parser.feed(body)
    parser.close()
    return str(sax)


class SaxTextifier:
    def __init__(self, ignore_tags={'noscript', 'meta', 'input', 'script', 'style'}):
        self.text = io.StringIO()
        self.ignore_tag_stack = []
        self.ignore_tags = ignore_tags

    def __str__(self):
        self.text.seek(0)
        return self.text.read()

    def data(self, data):
        if not self.ignore_tag_stack:
            self.text.write(data)

    def start(self, tag, attrs):
        tag = tag.lower()
        if tag in self.ignore_tags:
            self.ignore_tag_stack.append(tag)

    def end(self, tag):
        tag = tag.lower()
        if tag in self.ignore_tags:
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
