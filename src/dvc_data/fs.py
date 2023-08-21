import copy
import errno
import logging
import os
import typing
from typing import Any, BinaryIO, NamedTuple, Tuple

from dvc_objects.fs.callbacks import DEFAULT_CALLBACK
from fsspec import AbstractFileSystem

from .objects import cached_property

if typing.TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath, FileSystem
    from dvc_objects.fs.callbacks import Callback
    from dvc_objects.fs.path import Path

    from .index import DataIndex, ObjectStorage

logger = logging.getLogger(__name__)


class FileInfo(NamedTuple):
    typ: str
    storage: "ObjectStorage"
    cache_storage: "ObjectStorage"
    fs: "FileSystem"
    fs_path: "AnyFSPath"


class DataFileSystem(AbstractFileSystem):  # pylint:disable=abstract-method
    root_marker = "/"

    def __init__(self, index: "DataIndex", **kwargs: Any):
        super().__init__(**kwargs)
        self.index = index

    @cached_property
    def path(self) -> "Path":
        from dvc_objects.fs.path import Path

        def _getcwd() -> str:
            return self.root_marker

        return Path(self.sep, getcwd=_getcwd)

    def _get_key(self, path: str) -> Tuple[str, ...]:
        path = self.path.abspath(path)
        if path == self.root_marker:
            return ()

        key = self.path.relparts(path, self.root_marker)
        if key == (".",) or key == ("",):
            key = ()

        return key

    def _get_fs_path(self, path: "AnyFSPath") -> FileInfo:
        from .index import StorageKeyError

        info = self.info(path)
        if info["type"] == "directory":
            raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), path)

        entry = info["entry"]

        for typ in ["cache", "remote", "data"]:
            try:
                info = self.index.storage_map[entry.key]
                storage = getattr(info, typ)
                if not storage:
                    continue
                data = storage.get(entry)
            except (ValueError, StorageKeyError):
                continue
            if data:
                fs, fs_path = data
                if fs.exists(fs_path):
                    return FileInfo(typ, storage, info.cache, fs, fs_path)

        raise FileNotFoundError(errno.ENOENT, "No storage files available", path)

    def _cache_remote_file(
        self,
        cache_storage: "ObjectStorage",
        fs: "FileSystem",
        path: "AnyFSPath",
    ) -> Tuple["FileSystem", "AnyFSPath"]:
        from dvc_objects.fs.local import LocalFileSystem

        if isinstance(fs, LocalFileSystem):
            return fs, path

        from dvc_data.hashfile.build import _upload_file

        cache_odb = cache_storage.odb
        _, obj = _upload_file(path, fs, cache_odb, cache_odb)
        return cache_odb.fs, obj.path

    def _open(  # pylint: disable=arguments-differ
        self, path: "AnyFSPath", **kwargs: Any
    ) -> "BinaryIO":
        typ, _, cache_storage, fs, fspath = self._get_fs_path(path)

        if kwargs.get("cache", False) and typ == "remote" and cache_storage:
            fs, fspath = self._cache_remote_file(cache_storage, fs, fspath)

        return fs.open(fspath, mode="rb")

    def ls(self, path: "AnyFSPath", detail: bool = True, **kwargs: Any):
        root_key = self._get_key(path)
        try:
            info = self.index.info(root_key)
            if info["type"] != "directory":
                raise NotADirectoryError(path)

            if not detail:
                return [
                    self.path.join(path, key[-1])
                    for key in self.index.ls(root_key, detail=False)
                ]

            entries = []
            for key, info in self.index.ls(root_key, detail=True):
                info["name"] = self.path.join(path, key[-1])
                entries.append(info)
            return entries
        except KeyError as exc:
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), path
            ) from exc

    def info(self, path: "AnyFSPath", **kwargs: Any):
        key = self._get_key(path)

        try:
            info = self.index.info(key)
        except KeyError as exc:
            raise FileNotFoundError(
                errno.ENOENT,
                os.strerror(errno.ENOENT),
                path,
            ) from exc

        info["name"] = path
        return info

    def get_file(  # pylint: disable=arguments-differ
        self,
        rpath: "AnyFSPath",
        lpath: "AnyFSPath",
        callback: "Callback" = DEFAULT_CALLBACK,
        **kwargs: Any,
    ) -> None:
        from dvc_objects.fs.generic import transfer
        from dvc_objects.fs.local import LocalFileSystem

        from dvc_data.index import ObjectStorage

        try:
            typ, storage, cache_storage, fs, path = self._get_fs_path(rpath)
        except IsADirectoryError:
            os.makedirs(lpath, exist_ok=True)
            return None

        cache = kwargs.pop("cache", False)
        if cache and typ == "remote" and cache_storage:
            fs, path = self._cache_remote_file(cache_storage, fs, path)

        if (
            isinstance(storage, ObjectStorage)
            and isinstance(fs, LocalFileSystem)
            and storage.odb.cache_types
        ):
            try:
                transfer(
                    fs,
                    path,
                    fs,
                    os.fspath(lpath),
                    callback=callback,
                    links=copy.copy(storage.odb.cache_types),
                )
                return
            except OSError:
                pass

        fs.get_file(path, lpath, callback=callback, **kwargs)

    def checksum(self, path: str) -> str:
        info = self.info(path)
        md5 = info.get("md5")
        if md5:
            assert isinstance(md5, str)
            return md5
        raise NotImplementedError
