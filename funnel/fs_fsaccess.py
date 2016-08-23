import os
import fnmatch
import logging
from io import BytesIO
from cwltool.stdfsaccess import StdFsAccess

log = logging.getLogger('funnel')

class FsFsAccess(StdFsAccess):
    def __init__(self, base, storage):
        log.debug('FSAccess init ------------------ ' + base)
        self.base = base
        self.storage = storage

    def _abs(self, p):  # type: (unicode) -> unicode
        return os.path.abspath(os.path.join(self.storage, self.base, p))
    
    def protocol(self):
        return 'fs://' + self.base + '/'

    def glob(self, pattern):  # type: (unicode) -> List[unicode]
        return ["fs://%s" % self._abs(l) for l in glob.glob(self._abs(pattern))]

    def open(self, fn, mode):  # type: (unicode, str) -> BinaryIO
        return open(self._abs(fn), mode)

    def exists(self, fn):  # type: (unicode) -> bool
        return os.path.exists(self._abs(fn))

    def isfile(self, fn):  # type: (unicode) -> bool
        return os.path.isfile(self._abs(fn))

    def isdir(self, fn):  # type: (unicode) -> bool
        return os.path.isdir(self._abs(fn))

    def listdir(self, fn):  # type: (unicode) -> List[unicode]
        return [abspath(l, fn) for l in os.listdir(self._abs(fn))]

    def join(self, path, *paths):  # type: (unicode, *unicode) -> unicode
        return os.path.join(path, *paths)

    def realpath(self, path):  # type: (str) -> str
        return os.path.realpath(path)
