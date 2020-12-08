

class Docstore:
    def __init__(self, doc_cls, id_field='doc_id'):
        self._doc_cls = doc_cls
        self._id_field = id_field
        self._id_field_idx = doc_cls._fields.index(id_field)

    def get(self, doc_id, field=None):
        result = self.get_many([doc_id], field)
        if result:
            return result[doc_id]
        raise KeyError(f'doc_id={doc_id} not found')

    def get_many(self, doc_ids, field=None):
        result = {}
        field_idx = self._doc_cls._fields.index(field) if field is not None else None
        for doc in self.get_many_iter(doc_ids):
            if field is not None:
                result[doc.doc_id] = doc[field_idx]
            else:
                result[doc.doc_id] = doc
        return result

    def get_many_iter(self, doc_ids):
        raise NotImplementedError()

    def clear_cache(self):
        pass
