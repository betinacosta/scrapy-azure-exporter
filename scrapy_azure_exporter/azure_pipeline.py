from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
import typing
from scrapy.exceptions import NotConfigured
import logging

"""The pipeline for adding a file to Azure Blob"""


class AzureExporterPipeline:

    def __init__(self, container_name, service, azure_export_filename):
        self.container_name = container_name
        self.service = service
        self.azure_export_filename = azure_export_filename

    @classmethod
    def from_crawler(cls, crawler) -> typing.Any:
        container_name = crawler.settings.get("CONTAINER_NAME")
        connection_string = crawler.settings.get("CONNECTION_STRING")
        azure_export_filename = crawler.settings.get("AZURE_EXPORT_FILENAME")

        try:
            service = BlobServiceClient.from_connection_string(conn_str=connection_string)
        except ValueError as e:
            raise NotConfigured(
                f"Could not connect to Azure Client: {e}"
            )
        return cls(container_name=container_name, service=service, azure_export_filename=azure_export_filename)

    def process_item(self, item: typing.ByteString):
        try:
            blob_client = self.service.get_blob_client(self.container_name, self.azure_export_filename)
        except ResourceNotFoundError as e:
            logging.error(f"Container doesn't exist: {e}")
            return
        blob_client.upload_blob(item, blob_type="BlockBlob")
