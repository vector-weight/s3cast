import unittest
from unittest.mock import AsyncMock, patch, MagicMock
import aioboto3
import asyncio

from Untitled-1 import (
    calculate_dynamic_concurrency,
    copy_object_streaming,
    copy_bucket,
    main,
)

class TestAwsTransferScript(unittest.TestCase):

    @patch('os.cpu_count', return_value=4)
    @patch('psutil.virtual_memory')
    def test_calculate_dynamic_concurrency(self, mock_virtual_memory, mock_cpu_count):
        mock_virtual_memory.return_value.available = 16 * 1024 * 1024 * 1024  # 16 GB
        concurrency = calculate_dynamic_concurrency()
        self.assertEqual(concurrency, 8)  # 4 CPUs * 2 = 8

    @patch('aioboto3.Session.client')
    @patch('aioboto3.Session')
    @patch('Untitled-1.copy_object_streaming', new_callable=AsyncMock)
    def test_copy_bucket(self, mock_copy_object_streaming, mock_session, mock_client):
        source_session = mock_session.return_value
        dest_session = mock_session.return_value
        source_s3 = mock_client.return_value
        dest_s3 = mock_client.return_value

        source_s3.list_objects_v2 = AsyncMock(return_value={
            'Contents': [{'Key': 'test1.txt'}, {'Key': 'test2.txt'}],
            'IsTruncated': False
        })
        dest_s3.create_bucket = AsyncMock()
        dest_s3.create_multipart_upload = AsyncMock(return_value={'UploadId': '123'})
        dest_s3.upload_part = AsyncMock(return_value={'ETag': 'etag'})
        dest_s3.complete_multipart_upload = AsyncMock()

        asyncio.run(copy_bucket(source_session, dest_session, 'source-bucket', 2))

        source_s3.list_objects_v2.assert_called_once_with(Bucket='source-bucket', MaxKeys=1000)
        dest_s3.create_bucket.assert_called_once()
        self.assertEqual(mock_copy_object_streaming.call_count, 2)

    @patch('aioboto3.Session.client')
    @patch('aioboto3.Session')
    @patch('Untitled-1.calculate_dynamic_concurrency', return_value=2)
    @patch('sys.stdin.readline', return_value='source-bucket\n')
    def test_main(self, mock_readline, mock_calculate_dynamic_concurrency, mock_session, mock_client):
        source_session = mock_session.return_value
        dest_session = mock_session.return_value
        source_s3 = mock_client.return_value

        source_s3.list_buckets = AsyncMock(return_value={
            'Buckets': [{'Name': 'source-bucket'}]
        })

        with patch('Untitled-1.copy_bucket', new_callable=AsyncMock) as mock_copy_bucket:
            asyncio.run(main())
            source_s3.list_buckets.assert_called_once()
            mock_copy_bucket.assert_called_once_with(source_session, dest_session, 'source-bucket', 2)

if __name__ == '__main__':
    unittest.main()
