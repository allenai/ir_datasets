import os
import gzip
import ir_datasets

class WarcIndexFile:
    def __init__(self, fileobj, mode, doc_id_size=25):
        lz4 = ir_datasets.lazy_libs.lz4_frame()
        self.fileobj = lz4.frame.open(fileobj, mode, compression_level=lz4.frame.COMPRESSIONLEVEL_MAX)
        self.doc_id_size = doc_id_size

    def write(self, doc_id, state, pos, out_offset):
        zdict, bits, byte = state
        assert len(doc_id.encode()) == self.doc_id_size
        out = doc_id.encode() + \
              pos.to_bytes(4, 'little') + \
              bits.to_bytes(1, 'little') + \
              byte.to_bytes(1, 'little') + \
              zdict + \
              out_offset.to_bytes(4, 'little')
        self.fileobj.write(out)
        self.fileobj.flush()

    def read(self):
        ldid = self.doc_id_size
        chunk = f_chk.read(ldid + 4 + 1 + 1 + (32 * 1024) + 4)
        if not chunk:
            raise EOFError()
        did = chunk[:ldid].decode()
        pos = int.from_bytes(chunk[ldid:ldid+4], 'little')
        bits = int.from_bytes(chunk[ldid+4:ldid+4+1], 'little')
        byte = int.from_bytes(chunk[ldid+4+1:ldid+4+1+1], 'little')
        zdict = chunk[ldid+4+1+1:ldid+4+1+1+(32 * 1024)]
        out_offset = int.from_bytes(chunk[-4:], 'little')
        state = (zdict, bits, byte)
        return (doc_id, state, pos, out_offset)

    def __bool__(self):
        # TODO: a better way? Does lz4 handle peeks okay?
        return self.peek(1) != b''

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.fileobj.close()


class ClueWebWarcIndex:
    def __init__(self, source_path, index_path, id_field='WARC-TREC-ID'):
        self.source_path = source_path
        self.index_path = index_path
        self.zlib_state = ir_datasets.lazy_libs.zlib_state()
        self.id_field = id_field

    def build(self, checkpoint_freq=8*1024*1024):
        warc = ir_datasets.lazy_libs.warc()
        next_checkpoint = None
        last_chekpoint_pos = 0
        with self.zlib_state.GzipStateFile(self.source_path, keep_last_state=True) as f, \
             warc.WARCFile(fileobj=f) as f_warc, \
             ir_datasets.util.finialized_file(self.index_path, 'wb') as f_tmp, \
             WarcIndexFile(f_tmp, 'wb') as f_chk:
            for doc in f_warc:
                if doc.type == 'warcinfo':
                    continue
                if next_checkpoint:
                    state, pos, out_offset = next_checkpoint
                    f_chk.write(doc[self.id_field], state, pos - last_chekpoint_pos, out_offset)
                    last_chekpoint_pos = pos
                    next_checkpoint = None
                if f.last_state_pos and (f.last_state_pos >= last_chekpoint_pos + checkpoint_freq):
                    doc.payload.read() # advance the reader to the end of the current file
                    next_checkpoint = (f.last_state, f.last_state_pos, f.output_pos - f.last_state_output_pos + 4) # +4 for \r\n\r\n
                    if next_checkpoint[2] < 0:
                        next_checkpoint = None # split part way through the header... Skip this doc (will checkpoint in next iteration)

    def built(self):
        return os.path.exists(self.index_path)
