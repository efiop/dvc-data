from dvc_objects.db import ObjectDB

# FIXME OLOLO should we put cache into a separate dir and have build/load there to use in state?
from ..objects.cache import CacheObject


class CacheDB(ObjectDB):
    def get(self, oid: str):
        raw = super().get(oid)

        obj = CacheObject.load(raw.path, raw.fs)
        obj.oid = oid

        return obj
