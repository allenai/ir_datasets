# ClueWeb WARC Checkpoint Files

ClueWeb WARC Checkpoint files allow for fast random access from the ClueWeb source files.
The main idea is to take periodic checkpoints of the gzip decoding state, along with relevant
metadata to resume decoding from several checkpoints in the file. This means that for random
access, the entire file (up to the desird document) does not need to be decoded.

The checkpoints released with ir_datasets contain checkpoints at about every 8MB of compressed
data. This provided a reasonable trade-off between speed and data. The checkpoint files are about
0.1% the size of the original source files, and allow for about 40x faster random access, based on
my benchmarks.

## File Format

There is a `.chk.lz4` file for each `.warc.gz` source file. This file is a binary file, compressed
using the fast [lz4 compression format](https://github.com/lz4/lz4). This format was selected because
of its fast decompression, reasonable compression ratio, and self-contained pypi package.

When de-compressed, the file contains several 32807-byte chunks:

```
bytes	Type	Desc
25		string	Fixed-length document ID, UTF-8 encoded.
4		int		Index of document within file, as little-endian int
4		int		Position offset in *compressed* file, as little-endian int
        		This value increments from the previous offset, starting at 0
1		byte	gzip state "prime" bits
1		byte	gzip state "prime" bytes
32768	string	gzip state zdict
4		int		offset of *decompressed* buffer to start of the document, as little-endian int
```

## Example

File: `0000tw-00.warc.gz.chk.lz4`

```
[chunk 0]
25		'clueweb12-0000tw-00-02049'
4		2047
4		8398850
1		0x00
1		0xff
32768	b'iption" content="Time, experience and research findings sh[SNIP]ege mentors to research using the I'
4		11123
[chunk 1]
25		'clueweb12-0000tw-00-03082'
4		3069
4		8392388
1		0x07
1		0xd9
32768	b'irstChild.appendChild(ga);\n  })();\n</script>\n\n</head>\[SNIP]usiness Cards</a> at Vistaprint.com'
4		66946
...
```

This file contains documents prefixed with ID `clueweb12-0000tw-00-` (as indicated by the file name).

The 2047th document in the file is `clueweb12-0000tw-00-02049`. To jump to the start of this document,
you first seek to position 8398850 in the file. You then "prime" the gzip decoding state using bits of
`0x00`, byte `0xff`, and decoding dictionary of `b'iption" cont[SNIP]'`. You then read 11123 bytes
from the decompressed (inflated) stream to get to the start of this document.

The 3069th document in the file is `clueweb12-0000tw-00-03082`. To jump to the start of this document,
you first seek to position 16791238 (8398850+8392388) in the file. You then "prime" the gzip decoding
state using bits of `0x07`, byte `0xd9`, and decoding dictionary of `b'irstChild.ap[SNIP]`. You
then read 66946 bytes from the decompressed (inflated) stream to get to the start of this document.
