import os
import re
import ir_datasets
from .metadata import MetadataComponent


__all__ = 'Registry'
_logger = ir_datasets.log.easy()


class Registry:
    def __init__(self, allow_overwrite=False):
        self._registered = {}
        self._patterns = []
        self._allow_overwrite = allow_overwrite

    def __getitem__(self, key):
        if key not in self._registered:
            for pattern, initializer in self._patterns:
                match = pattern.match(key)
                if match:
                    dataset = initializer(key, match.groups())
                    self.register(key, dataset)
                    break
        result = self._registered[key]
        if hasattr(result, 'deprecated'):
            if os.environ.get('IR_DATASETS_SKIP_DEPRECATED_WARNING', '').lower() != 'true':
                _logger.info(result.deprecated())
        return result

    def __iter__(self):
        return iter(self._registered.keys())

    def register(self, name, obj):
        from ..datasets.base import Dataset
        if name in self._registered:
            if self._allow_overwrite:
                _logger.warn(f"{name} already exists in this registry. Overwriting.")
            else:
                raise RuntimeError(f"{name} already exists in this registry.")
        if not hasattr(obj, 'metadata'):
            obj = Dataset(MetadataComponent(name, obj), obj) # add metadata from default provider
        self._registered[name] = obj

    def register_pattern(self, pattern, initializer):
        self._patterns.append((re.compile(pattern), initializer))
