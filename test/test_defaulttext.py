import unittest
import ir_datasets

def template_instance(Cls):
    if hasattr(Cls, '_fields'):
        return Cls(*[template_instance(v) for k, v in Cls.__annotations__.items()])
    elif hasattr(Cls, '_name') and Cls._name == 'List':
        return [template_instance(Cls.__args__[0]) for _ in range(3)]
    elif hasattr(Cls, '_name') and Cls._name == 'Tuple':
        return tuple([template_instance(Cls.__args__[0]) for _ in range(3)])
    elif hasattr(Cls, '__members__'):
        return Cls(next(iter(Cls.__members__.values())))
    elif Cls is str:
        return 'test string'
    else:
        try:
            return Cls()
        except:
            return None

class TestMetadata(unittest.TestCase):
    def test_all_defualttext(self):
        for dsid in ir_datasets.registry._registered:
            self._test_defaulttet(dsid)

    def _test_defaulttet(self, dsid):
        with self.subTest(dsid):
            dataset = ir_datasets.load(dsid)
            if dataset.has_docs():
                Cls = dataset.docs_cls()
                instance = template_instance(Cls)
                if hasattr(instance, 'default_text'):
                    instance.default_text() # test it doesn't raise an error
                else:
                    print(dsid, 'missing doc default_text')
            if dataset.has_queries():
                Cls = dataset.queries_cls()
                instance = template_instance(Cls)
                if hasattr(instance, 'default_text'):
                    instance.default_text() # test it doesn't raise an error
                else:
                    print(dsid, 'missing query default_text')


if __name__ == '__main__':
    unittest.main()
