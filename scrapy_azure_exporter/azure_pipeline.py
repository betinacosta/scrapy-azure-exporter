

class AzureExporterPipeline(object):

    AZURE_ACCOUNT_NAME = None
    AZURE_SECRET_ACCESS_KEY = None

    HEADERS = {
        'Cache-Control': 'max-age=172800',
    }

    def __init__(self, uri):
        from azure.storage.blob import BlockBlobService, ContentSettings
        self.BlockBlobService = BlockBlobService
        self.ContentSettings = ContentSettings

        assert uri.startswith('azure://')
        self.container, self.prefix = uri[8:].split('/', 1)

    def stat_file(self, path, info):
        def _onsuccess(blob_properties):
            if blob_properties:
                checksum = blob_properties.properties.etag.strip('"')
                last_modified = blob_properties.properties.last_modified  # Aware dt
                modified_tuple = parsedate_tz(last_modified.strftime("%d %b %Y %H:%M:%S %z"))
                modified_stamp = int(mktime_tz(modified_tuple))
                return {'checksum': checksum, 'last_modified': modified_stamp}
            return None

        return self._get_azure_blob(path).addCallback(_onsuccess)

    def _get_azure_service(self):
        return self.BlockBlobService(account_name=self.AZURE_ACCOUNT_NAME,
                                     account_key=self.AZURE_SECRET_ACCESS_KEY)

    def _get_azure_blob(self, path):
        blob_name = '%s%s' % (self.prefix, path)
        service = self._get_azure_service()

        if service.exists(self.container, blob_name=blob_name):
            # Get properties
            return threads.deferToThread(service.get_blob_properties, self.container, blob_name=blob_name)
        return threads.deferToThread(lambda _: _, None)

    def process_item(self, path, buf, info, meta=None, headers=None):
        """Upload file to Azure blob storage"""
        blob_name = '%s%s' % (self.prefix, path)
        extra = self._headers_to_azure_content_kwargs(self.HEADERS)
        if headers:
            extra.update(self._headers_to_azure_content_kwargs(headers))
        buf.seek(0)
        s = self._get_azure_service()
        return threads.deferToThread(s.create_blob_from_bytes, self.container, blob_name, buf.getvalue(),
                                     metadata={k: str(v) for k, v in six.iteritems(meta or {})},
                                     content_settings=self.ContentSettings(**extra))

    def _headers_to_azure_content_kwargs(self, headers):
        """ Convert headers to Azure content settings keyword agruments.
        """
        # This is required while we need to support both boto and botocore.
        mapping = CaselessDict({
            'Content-Type': 'content_type',
            'Cache-Control': 'cache_control',
            'Content-Disposition': 'content_disposition',
            'Content-Encoding': 'content_encoding',
            'Content-Language': 'content_language',
            'Content-MD5': 'content_md5',
            })
        extra = {}
        for key, value in six.iteritems(headers):
            try:
                kwarg = mapping[key]
            except KeyError:
                raise TypeError(
                    'Header "%s" is not supported by Azure' % key)
            else:
                extra[kwarg] = value
        return extra