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


def sax_html_parser(body, headers=None, force_encoding=None, fields=None):
    if fields is None:
        fields = [{'title'}, None]
    etree = ir_datasets.lazy_libs.lxml_html().etree
    sax = SaxExtractor(fields=fields)
    parser = etree.HTMLParser(target=sax)
    if isinstance(body, bytes):
        if force_encoding is None:
            body = decode_html(body, headers)
        else:
            body = body.decode(force_encoding, errors='ignore')
    parser.feed(body)
    parser.close()
    return sax.get_values()


class SaxExtractor:
    IGNORE_TAGS = {'noscript', 'meta', 'input', 'script', 'style'}
    def __init__(self, fields):
        self.fields = fields
        self.field_values = [[] for _ in fields]
        self.field_stacks = [deque() if f is not None else None for f in fields]
        self.ignore_tag_stack = deque()

    def get_values(self):
        return tuple(self._join_text(v) for v in self.field_values)

    def _join_text(self, text):
        res = ''.join(text)
        res = res.replace('\r\n', '\n').replace('\r', '\n') # CR/LF normalisation
        res = res.replace('\t', ' ') # tab/space normalisation
        res = re.sub('\n +', '\n', res) # remove spaces from start of lines
        res = re.sub(' +\n', '\n', res) # remove spaces from end of lines
        res = re.sub('\n{2,}', '\n', res) # collapse multiple empty lines
        res = re.sub(' {2,}', ' ', res)  # collapse multiple spaces
        return res.strip() # remove final leading/trailing whitespace

    def data(self, data):
        if not self.ignore_tag_stack:
            any_match = False
            for vals, stack in zip(self.field_values, self.field_stacks):
                if (stack is None and not any_match) or stack:
                    vals.append(data)
                    any_match = True

    def start(self, tag, attrs):
        tag = tag.lower()
        for tags, stack in zip(self.fields, self.field_stacks):
            if tags is not None and tag in tags:
                stack.append(tag)
        if tag in self.IGNORE_TAGS:
            self.ignore_tag_stack.append(tag)

    def end(self, tag):
        tag = tag.lower()
        for stack in self.field_stacks:
            if stack and stack[-1] == tag:
                stack.pop()
        if self.ignore_tag_stack and self.ignore_tag_stack[-1] == tag:
            self.ignore_tag_stack.pop()

    def close(self):
        pass

    def comment(self, data):
        pass

    def doctype(self, *args):
        pass

    def pi(self, *args):
        pass
