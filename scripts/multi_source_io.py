"""Shared fix for a reproduced, documented gap: a scan of a no-rewrite
consolidated table can need to physically open a data file that still lives
under a SOURCE table's own storage location, not the target's.

Why this is needed: register_consolidation.py's whole technique is splicing
a source's Parquet files into a target table's manifest BY REFERENCE -- the
physical files never move (see that script's module docstring). Polaris
(like other Iceberg REST catalogs) only ever vends a table's own AWS
credential scoped to that table's OWN default storage location (see
catalog_properties() in register_consolidation.py). So any scan of the
target table that needs to physically read a spliced-in file -- not just
plan/list it from the manifest, which stays metadata-only against the
target's own storage location and always works -- fails with an AWS
ACCESS_DENIED unless it's given a credential that also covers that file's
real, physical location.

Reproduced live and documented in CHANGELOG.md's "Known issue:
collision-rewrite path can hit ACCESS_DENIED under vended credentials" --
this module is the fix, used by register_consolidation.py's collision
idempotency check and verify_consolidation.py's row-count check, the two
places a cross-namespace physical read happens today.
"""
from urllib.parse import urlparse

from pyiceberg.io import FileIO


class MultiSourceFileIO(FileIO):
    """Wraps several tables' own FileIO instances (each carrying that
    table's own vended credential) and tries each in turn to open a file,
    caching which one worked per storage prefix so repeat opens of the same
    source's files don't retry every candidate.

    Only affects READS. Writes and deletes always go through the first
    candidate (intended to be the target table's own IO) -- this class
    exists to make cross-namespace reads work, not to change where data
    gets written.
    """

    def __init__(self, ios):
        if not ios:
            raise ValueError("MultiSourceFileIO needs at least one FileIO")
        super().__init__(ios[0].properties)
        self._ios = ios
        self._cache = {}

    @staticmethod
    def _prefix_key(location: str) -> str:
        # scheme://netloc/first-two-path-segments -- coarse enough to
        # identify "which table's storage location is this" without
        # depending on any catalog-specific path convention, fine enough
        # that different sources sharing one bucket (a real, observed
        # Polaris layout: one bucket, one key prefix per namespace) don't
        # collide in the cache.
        u = urlparse(location)
        segments = u.path.strip("/").split("/")[:2]
        return f"{u.scheme}://{u.netloc}/{'/'.join(segments)}"

    def new_input(self, location: str):
        key = self._prefix_key(location)
        cached = self._cache.get(key)
        if cached is not None:
            return cached.new_input(location)

        last_exc = None
        for io in self._ios:
            try:
                io.new_input(location).open().close()
                self._cache[key] = io
                return io.new_input(location)
            except Exception as e:  # noqa: BLE001 -- deliberately broad, see module docstring
                last_exc = e
                continue
        raise last_exc

    def new_output(self, location: str):
        return self._ios[0].new_output(location)

    def delete(self, location):
        return self._ios[0].delete(location)
