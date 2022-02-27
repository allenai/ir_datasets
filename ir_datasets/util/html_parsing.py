from collections import deque
import re
import io
import ir_datasets


def find_charset(text):
    if text is None:
        return None
    if isinstance(text, str):
        text = text.encode()
    try:
        idx = text.index(b'charset=')
        match = re.match(b'charset= *["\']?([a-zA-Z0-9-_]+)', text[idx:])
        if match:
            return match.group(1).decode(errors='ignore')
    except ValueError:
        pass
    return None


def decode_html(body, headers=None):
    for encoding in [find_charset(body), find_charset(headers), 'utf8']:
        if encoding is not None:
            try:
                return body.decode(encoding, errors='ignore')
            except LookupError: # charset not found
                pass # continue on to next encoding -- utf8 will always be found


def sax_html_parser(body, headers=None, title_separate=True, force_encoding=None, title_tag='title'):
    etree = ir_datasets.lazy_libs.lxml_html().etree
    sax = SaxExtractor(title_separate=title_separate, title_tag=title_tag)
    parser = etree.HTMLParser(target=sax)
    if isinstance(body, bytes):
        if force_encoding is None:
            body = decode_html(body, headers)
        else:
            body = body.decode(force_encoding, errors='ignore')
    parser.feed(body)
    parser.close()
    if title_separate:
        return sax.title, str(sax)
    return str(sax)


class SaxExtractor:
    IGNORE_TAGS = {'noscript', 'meta', 'input', 'script', 'style'}
    def __init__(self, title_separate=True, title_tag='title'):
        self.text = []
        self.title = '' if title_separate else None
        self.ignore_tag_stack = deque()
        self.title_tag = title_tag
        self.title_separate = title_separate
        self.in_title = False

    def __str__(self):
        # Join and clean up the text
        res = ''.join(self.text)
        res = res.replace('\r\n', '\n').replace('\r', '\n') # CR/LF normalisation
        res = res.replace('\t', ' ') # tab/space normalisation
        res = re.sub('\n +', '\n', res) # remove spaces from start of lines
        res = re.sub(' +\n', '\n', res) # remove spaces from end of lines
        res = re.sub('\n{2,}', '\n', res) # collapse multiple empty lines
        res = re.sub(' {2,}', ' ', res)  # collapse multiple spaces
        return res

    def data(self, data):
        if self.in_title:
            self.title += data
        elif not self.ignore_tag_stack:
            self.text.append(data)

    def start(self, tag, attrs):
        tag = tag.lower()
        if tag in self.IGNORE_TAGS:
            self.ignore_tag_stack.append(tag)
        elif self.title_separate and tag == self.title_tag:
            self.in_title = True

    def end(self, tag):
        tag = tag.lower()
        while self.ignore_tag_stack and self.ignore_tag_stack[-1] == tag:
            self.ignore_tag_stack.pop()
        if self.title_separate and tag == self.title_tag:
            self.in_title = False

    def close(self):
        pass

    def comment(self, data):
        pass

    def doctype(self, *args):
        pass

    def pi(self, *args):
        pass
