import ir_datasets


__all__ = 'Registry'
_logger = ir_datasets.log.easy()


class Registry:
    def __init__(self, allow_overwrite=False):
        self._registered = {}
        self._allow_overwrite = allow_overwrite

    def __getitem__(self, key):
        return self._registered[key]

    def __iter__(self):
        return iter(self._registered.keys())

    def register(self, name, obj):
        if name in self._registered:
            if self._allow_overwrite:
                _logger.warn(f"{name} already exists in this registry. Overwriting.")
            else:
                raise RuntimeError(f"{name} already exists in this registry.")
        self._registered[name] = obj
