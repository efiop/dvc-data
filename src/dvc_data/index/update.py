from typing import TYPE_CHECKING, List, Optional

from .diff import UNCHANGED, diff
from .index import Meta

if TYPE_CHECKING:
    from .index import BaseDataIndex, DataIndex


def update(new: "DataIndex", old: "BaseDataIndex") -> None:
    for change in diff(old, new, with_unchanged=True, meta_only=True):
        if change.typ == UNCHANGED:
            change.new.hash_info = change.old.hash_info


def update_meta(index: "BaseDataIndex", storages: Optional[List[str]] = None):

    if storages is None:
        storages = ["cache"]

    updates = []
    for key, entry in index.iteritems():
        if entry.meta and entry.meta.isdir:
            continue

        storage_info = index.storage_map.get(entry.key)
        if not storage_info:
            continue

        for name in storages:
            storage = getattr(storage_info, name, None)
            if not storage:
                continue

            data = storage.get(entry)
            if not data:
                continue

            fs, fs_path = data
            try:
                info = fs.info(fs_path)
            except FileNotFoundError:
                continue
            meta = Meta.from_info(info, fs.protocol)

            updates.append((key, meta))

    for key, meta in updates:
        entry = index[key]
        entry.meta = meta
        index[key] = entry

    index.commit()
