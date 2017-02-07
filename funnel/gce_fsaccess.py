import os
import fnmatch
import logging
from io import BytesIO
try:
    from gcloud import storage
except ImportError:
    storage = None
from cwltool.stdfsaccess import StdFsAccess

log = logging.getLogger('funnel')

class GCEFsAccess(StdFsAccess):
    def __init__(self, base):
        log.debug('GCEFSACCESS init ------------------ ' + base)

        self.base = base
        self.client = storage.Client()
        self.bucket = self.client.get_bucket(self.base)

    def protocol(self):
        return 'gs://' + self.base + '/'

    def _abs(self, path):
        log.debug('GCEFSACCESS abs ------------------ ' + path)
        return os.path.join('gs://', self.base, path)

    def blob(self, path):
        log.debug('GCEFSACCESS blob ------------------ ' + path)
        return self.bucket.get_blob(path)

    def list(self):
        log.debug('GCEFSACCESS list ------------------ ' + self.base)
        self.bucket.reload()
        blobs = []
        for blob in self.bucket.list_blobs():
            blobs.append(blob)

        return blobs

    def glob(self, pattern):
        log.debug('GCEFSACCESS glob ------------------ ' + pattern)
        self.bucket.reload()
        return [self._abs(blob) for blob in self.bucket.list_blobs() if fnmatch.fnmatch(blob, pattern)]

    def open(self, path, mode):
        log.debug('GCEFSACCESS open ------------------ ' + path + ' ' + mode)
        blob = self.blob(path)
        stream = BytesIO()
        return blob.download_to_file(stream)

    def exists(self, path):
        log.debug('GCEFSACCESS exists ------------------ ' + path)
        blob = self.blob(path)
        if blob:
            return blob.exists()

    def isfile(self, path):
        log.debug('GCEFSACCESS isfile ------------------ ' + path)
        blob = self.blob(path)
        if blob and blob.exists():
            return not path[-1] == '/'

    def isdir(self, path):
        log.debug('GCEFSACCESS isdir ------------------ ' + path)
        return len(self.listdir(path)) > 0

    def listdir(self, path):
        log.debug('GCEFSACCESS listdir ------------------ ' + path)

        if not path[-1] == '/':
            path = path + '/'

        protocol = self.protocol()
        if path.startswith(protocol):
            path = path[len(protocol):]

        self.bucket.reload()
        blobs = self.bucket.list_blobs()
        return [self._abs(blob.name) for blob in blobs if blob.name.startswith(path)]

    def join(self, path, *paths):
        return os.path.join(path, *paths)
