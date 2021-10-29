from azure.storage.blob import BlobClient
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import generate_blob_sas, BlobSasPermissions


class Azure:
    """
    Simple Azure Storage client.
    References:
    https://azure.microsoft.com/en-us/services/storage/blobs/#overview
    https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python
    """

    def __init__(self, connection_string, container_name, expiry, account_name, account_key):
        """
        :param connection_string: authorization information required for your application to access data.
               See: https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string
        :type connection_string: str
        :param container_name: container name where the blob will be saved
        :type container_name: str
        :param expiry: expiration date (relative to the current date)
        :type expiry: datetime.timedelta
        :param account_name: name of the account responsible for the container
        :type account_name: str
        :param account_key: key of the respective account
        :type account_key: str
        """
        self.connection_string = connection_string
        self.container_name = container_name
        self.expiry = expiry
        self.account_name = account_name
        self.account_key = account_key

    def upload(self, filename, data):
        """
        Upload the file to Azure Storage and return a SAS URL.

        :param filename: name/key to use when uploading the file to the container
        :type filename: str
        :param data: file-like object used as datasource
        :type data: file-like object
        :returns: SAS URL pointing to the uploaded file
        :rtype: str
        """
        blob_client = BlobClient.from_connection_string(
            self.connection_string, container_name=self.container_name, blob_name=filename)
        blob_client.upload_blob(data)
        return self.signed_url(filename)

    def get_blob(self, filename):
        """
        Get a blob object pointing to the file path, ``None`` if
        the file doesn't exist.

        :param filename: string with name/path of the file to download
        :return: a blob like object
        See https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python
        """
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        container_client = blob_service_client.get_container_client(self.container_name)
        blob_client = container_client.get_blob_client(filename)

        return blob_client.download_blob()

    def signed_url(self, blob_name):
        """
        Get a SAS URL.
        https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string

        :param blob_name: blob name, aka name of the file you just uploaded
        :type blob_name: str
        :returns: SAS URL
        :rtype: str
        """
        sas_blob = generate_blob_sas(
            account_name=self.account_name,
            container_name=self.container_name,
            blob_name=blob_name,
            account_key=self.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=self.expiry
        )
        url = 'https://' + self.account_name + '.blob.core.windows.net/' + self.container_name + '/' + blob_name + '?' + sas_blob
        return url



