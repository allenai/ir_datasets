import io
import os
import gzip
from contextlib import ExitStack
import ir_datasets
from . import Docstore

class WarcIndexFile:
    def __init__(self, fileobj, mode, doc_id_size=25):
        lz4 = ir_datasets.lazy_libs.lz4_frame()
        self.fileobj = lz4.frame.open(fileobj, mode, compression_level=lz4.frame.COMPRESSIONLEVEL_MAX)
        self.doc_id_size = doc_id_size
        self.pos = 0

    def write(self, doc_id, doc_idx, state, pos, out_offset):
        zdict, bits, byte = state
        assert len(doc_id.encode()) == self.doc_id_size
        out = doc_id.encode() + \
              doc_idx.to_bytes(4, 'little') + \
              pos.to_bytes(4, 'little') + \
              bits.to_bytes(1, 'little') + \
              byte.to_bytes(1, 'little') + \
              zdict + \
              out_offset.to_bytes(4, 'little')
        self.fileobj.write(out)
        self.fileobj.flush()

    def read(self):
        ldid = self.doc_id_size
        chunk = self.fileobj.read(ldid + 4 + 4 + 1 + 1 + (32 * 1024) + 4)
        if not chunk:
            raise EOFError()
        chunk = io.BytesIO(chunk)
        doc_id = chunk.read(ldid).decode()
        doc_idx = int.from_bytes(chunk.read(4), 'little')
        pos = self.pos + int.from_bytes(chunk.read(4), 'little')
        bits = int.from_bytes(chunk.read(1), 'little')
        byte = int.from_bytes(chunk.read(1), 'little')
        zdict = chunk.read(32 * 1024)
        out_offset = int.from_bytes(chunk.read(4), 'little')
        state = (zdict, bits, byte)
        self.pos = pos
        return (doc_id, doc_idx, state, pos, out_offset)

    def peek_doc_id(self):
        return self.fileobj.peek(self.doc_id_size)[:self.doc_id_size].decode()

    def peek_doc_idx(self):
        int_bytes = self.fileobj.peek(self.doc_id_size + 4)[self.doc_id_size:self.doc_id_size+4]
        if int_bytes == b'':
            return None
        return int.from_bytes(int_bytes, 'little')

    def __bool__(self):
        # TODO: a better way? Does lz4 handle peeks okay?
        return self.fileobj.peek(1) != b''

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.fileobj.close()


class ClueWebWarcIndex:
    def __init__(self, source_path, index_path, id_field='WARC-TREC-ID', warc_cw09=False):
        self.source_path = source_path
        self.index_path = index_path
        self.zlib_state = ir_datasets.lazy_libs.zlib_state()
        self.id_field = id_field
        self.warc_cw09 = warc_cw09

    def build(self, checkpoint_freq=8*1024*1024):
        warc = ir_datasets.lazy_libs.warc_clueweb09() if self.warc_cw09 else ir_datasets.lazy_libs.warc()
        next_checkpoint = None
        last_chekpoint_pos = 0
        with self.zlib_state.GzipStateFile(self.source_path, keep_last_state=True) as f, \
             warc.WARCFile(fileobj=f) as f_warc, \
             ir_datasets.util.finialized_file(self.index_path, 'wb') as f_tmp, \
             WarcIndexFile(f_tmp, 'wb') as f_chk:
            doc_idx = 0
            for doc in f_warc:
                if doc.type == 'warcinfo':
                    continue
                if next_checkpoint:
                    state, pos, out_offset = next_checkpoint
                    f_chk.write(doc[self.id_field], doc_idx, state, pos - last_chekpoint_pos, out_offset)
                    last_chekpoint_pos = pos
                    next_checkpoint = None
                if f.last_state_pos and (f.last_state_pos >= last_chekpoint_pos + checkpoint_freq):
                    doc.payload.read() # advance the reader to the end of the current file
                    next_checkpoint = (f.last_state, f.last_state_pos, f.output_pos - f.last_state_output_pos + 4) # +4 for \r\n\r\n
                    if next_checkpoint[2] < 0:
                        next_checkpoint = None # split part way through the header... Skip this doc (will checkpoint in next iteration)
                doc_idx += 1

    def built(self):
        return os.path.exists(self.index_path)

    def get_many_iter(self, doc_ids, docs_obj):
        doc_ids = sorted(set(doc_ids))
        with ExitStack() as stack:
            f = stack.enter_context(self.zlib_state.GzipStateFile(self.source_path))
            f_chk = stack.enter_context(WarcIndexFile(self.index_path, 'rb'))
            state, pos, out_offset = None, None, None
            while doc_ids and f_chk:
                next_doc_id = f_chk.peek_doc_id()
                if doc_ids[0] < next_doc_id:
                    if state is not None:
                        f.zseek(pos, state)
                        f.read(out_offset)
                    doc_iter = docs_obj._docs_ctxt_iter_warc(f)
                    for doc in doc_iter:
                        brk = False
                        while doc.doc_id >= doc_ids[0]:
                            if doc.doc_id == doc_ids[0]:
                                yield doc
                            doc_ids = doc_ids[1:] # pop -- either not found or found
                            if not doc_ids or doc_ids[0] >= next_doc_id:
                                brk = True
                                break
                        if brk:
                            break
                doc_id, doc_idx, state, pos, out_offset = f_chk.read()



class ClueWebWarcDocstore(Docstore):
    def __init__(self, warc_docs):
        super().__init__(warc_docs.docs_cls(), 'doc_id')
        self.warc_docs = warc_docs

    def get_many_iter(self, doc_ids):
        result = {}
        files_to_search = {}
        for doc_id in doc_ids:
            source_file = self.warc_docs._docs_id_to_source_file(doc_id)
            if source_file is not None:
                if source_file not in files_to_search:
                    files_to_search[source_file] = []
                files_to_search[source_file].append(doc_id)
        for source_file, doc_ids in files_to_search.items():
            doc_ids = sorted(doc_ids)
            checkpoint_file = self.warc_docs._docs_source_file_to_checkpoint(source_file)
            if checkpoint_file:
                index = ClueWebWarcIndex(source_file, checkpoint_file)
                yield from index.get_many_iter(doc_ids, self.warc_docs)
            else:
                for doc in self.warc_docs._docs_ctxt_iter_warc(source_file):
                    if doc_ids[0] == doc.doc_id:
                        yield doc
                        doc_ids = doc_ids[1:]
                        if not doc_ids:
                            break # file finished

class WarcIter:
    def __init__(self, warc_docs, slice):
        self.next_index = 0
        self.warc_docs = warc_docs
        self.slice = slice
        self.current_file = None
        self.current_file_source = None
        self.current_file_start_idx = 0
        self.current_file_end_idx = 0
        self.current_chk = None
        self.file_iter = warc_docs._docs_iter_source_files()

    def __next__(self):
        if self.slice.start >= self.slice.stop:
            raise StopIteration
        while self.next_index != self.slice.start or self.current_file is None or self.current_file_end_idx <= self.slice.start:
            if self.current_file is None or self.current_file_end_idx <= self.slice.start:
                # First iteration or no docs remaining in this file
                if self.current_file is not None:
                    self.current_file.close()
                    self.current_file_source.close()
                    self.current_file = None
                    self.current_file_source = None
                if self.current_chk:
                    self.current_chk.close()
                    self.current_chk = None
                # jump ahead to the file that contains the desired index
                first = True
                while first or self.current_file_end_idx < self.slice.start:
                    source_file = next(self.file_iter)
                    self.next_index = self.current_file_end_idx
                    self.current_file_start_idx = self.current_file_end_idx
                    self.current_file_end_idx = self.current_file_start_idx + self.warc_docs._docs_warc_file_counts()[source_file]
                    first = False
                self.current_file_source = ir_datasets.lazy_libs.zlib_state().GzipStateFile(source_file)
                self.current_file = self.warc_docs._docs_ctxt_iter_warc(self.current_file_source)
                checkpoint_file = self.warc_docs._docs_source_file_to_checkpoint(source_file)
                if checkpoint_file:
                    self.current_chk = WarcIndexFile(checkpoint_file, 'rb')
            elif self.current_chk and self.current_file_start_idx + self.current_chk.peek_doc_idx() < self.slice.start:
                # We have a checkpoint file and the next block starts after this checkpoint.
                # So we can use zseek to jump ahead to it.
                while self.current_chk and self.current_file_start_idx + self.current_chk.peek_doc_idx() < self.slice.start:
                    doc_id, doc_idx, state, pos, out_offset = self.current_chk.read()
                self.current_file_source.zseek(pos, state)
                self.current_file_source.read(out_offset)
                self.next_index = self.current_file_start_idx + doc_idx
            else:
                # No checkpoint file available or as far as we can get with checkpoint; do slow read ahead
                for _ in zip(range(self.slice.start - self.next_index), self.current_file):
                    # The zip here will stop at after either as many docs we must advance, or however
                    # many docs remain in the file. In the latter case, we'll just drop out into the
                    # next iteration of the while loop and pick up the next file.
                    self.next_index += 1
        result = next(self.current_file)
        self.next_index += 1
        self.slice = slice(self.slice.start + (self.slice.step or 1), self.slice.stop, self.slice.step)
        return result

    def close(self):
        if self.current_file_source is not None:
            self.current_file_source.close()
            self.current_file_source = None
        if self.current_file is not None:
            self.current_file.close()
            self.current_file = None
        if self.current_chk:
            self.current_chk.close()
            self.current_chk = None
        self.file_iter = None

    def __iter__(self):
        return self

    def __del__(self):
        self.close()

    def __getitem__(self, key):
        if isinstance(key, slice):
            # it[start:stop:step]
            new_slice = ir_datasets.util.apply_sub_slice(self.slice, key)
            return WarcIter(self.warc_docs, new_slice)
        elif isinstance(key, int):
            # it[index]
            new_slice = ir_datasets.util.slice_idx(self.slice, key)
            new_it = WarcIter(self.warc_docs, new_slice)
            try:
                return next(new_it)
            except StopIteration as e:
                raise IndexError((self.slice, slice(key, key+1), new_slice))
        raise TypeError('key must be int or slice')
