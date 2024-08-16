from typing import Optional

from .file import LoadedFile


class FileCollection(dict):
    def __init__(self, seq, auto_rewind: bool = True, **kwargs):
        super().__init__(seq, **kwargs)

        self._auto_rewind = auto_rewind

    def get_file(
        self,
        data_feed_key: Optional[str] = None,
        data_spec_version: Optional[str] = None,
        auto_rewind: Optional[bool] = None,
    ) -> Optional[LoadedFile]:
        if not data_feed_key and not data_spec_version:
            raise ValueError(
                "You have to specify `data_feed_key` or `data_spec_version`"
            )

        for file in self.values():
            if (not data_feed_key or file.data_feed_key == data_feed_key) and (
                not data_spec_version or file.data_spec_version == data_spec_version
            ):
                should_auto_rewind = auto_rewind
                if should_auto_rewind is None:
                    should_auto_rewind = self._auto_rewind

                if should_auto_rewind and file.stream.tell() > 0:
                    file.stream.seek(0)
                return file

        return None
