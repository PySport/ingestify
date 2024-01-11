from typing import Optional

from .file import LoadedFile


class FileCollection(dict):
    def get_file(
        self,
        data_feed_key: Optional[str] = None,
        data_spec_version: Optional[str] = None,
    ) -> Optional[LoadedFile]:
        if not data_feed_key and not data_spec_version:
            raise ValueError(
                "You have to specify `data_feed_key` or `data_spec_version`"
            )

        for file in self.values():
            if (not data_feed_key or file.data_feed_key == data_feed_key) and (
                not data_spec_version or file.data_spec_version == data_spec_version
            ):
                return file

        return None
