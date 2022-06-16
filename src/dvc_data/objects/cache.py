import json

from dvc_objects.errors import ObjectFormatError
from dvc_objects.obj import Object

from dvc_data.hashfile.hash_info import HashInfo


# FIXME maybe HashObject? HashCacheObject? :D
class CacheObject(Object):
    def __init__(self, checksum, hash_info):
        self.path = None
        self.fs = None
        self.oid = None

        self.checksum = checksum
        self.hash_info = hash_info

    def as_dict(self):
        return {
            "checksum": self.checksum,
            "hash_info": self.hash_info.to_dict(),
        }

    def as_bytes(self):
        return json.dumps(self.as_dict(), sort_keys=True).encode("utf-8")

    def serialize(self):
        from dvc_objects.fs import MemoryFileSystem
        from dvc_objects.fs.utils import tmp_fname

        self.fs = MemoryFileSystem()
        self.path = "memory://{}".format(tmp_fname(""))

        # FIXME detach from global. Big perf hit
        self.fs.fs.store = {}
        self.fs.fs.pseudo_dirs = [""]

        self.fs.pipe_file(self.path, self.as_bytes())

    @classmethod
    def from_dict(cls, dict_):
        checksum = dict_["checksum"]
        hash_info = HashInfo.from_dict(dict_["hash_info"])
        return cls(checksum, hash_info)

    @classmethod
    def from_bytes(cls, byts):
        try:
            data = json.loads(byts.decode("utf-8"))
        except ValueError as exc:
            raise ObjectFormatError("ReferenceObject is corrupted") from exc

        return cls.from_dict(data)

    @classmethod
    def load(cls, path, fs):
        byts = fs.cat_file(path)

        obj = cls.from_bytes(byts)
        obj.path = path
        obj.fs = fs

        return obj
