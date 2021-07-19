# IR-datasets data types

# https://ir-datasets.com/


import datamaestro


class Repository(datamaestro.Repository):
    NAMESPACE = "irds"
    DESCRIPTION = "Information Retrieval datasets repository from https://ir-datasets.com/"

    def __init__(self, context):
        super().__init__(context)
        self._modules = None
        self._datasets = None

    def _check(self):
        from .datasets import build
        if self._modules is None:
            self._modules, self._datasets = build(self)

    def modules(self):
        self._check()
        yield from self._modules

    def search(self, name: str):
        self._check()
        return self._datasets.get(name, None)