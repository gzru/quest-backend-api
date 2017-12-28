import boto3


class S3Connector(object):

    def __init__(self, endpoint_url, access_key_id, secret_access_key):
        self._client = boto3.client('s3',
                                    endpoint_url=endpoint_url,
                                    aws_access_key_id=access_key_id,
                                    aws_secret_access_key=secret_access_key)

    def put_data_object(self, bucket, key, data):
        try:
            return self._client.put_object(ACL='public-read', ContentType='application/octet-stream', Bucket=bucket, Key=key, Body=data)
        except Exception as ex:
            # TODO
            raise

    def get_meta(self, bucket, key):
        try:
            return self._client.head_object(Bucket=bucket, Key=key)
        except Exception as ex:
            # TODO
            raise


if __name__ == "__main__":
    s = S3Connector()
    #print s.put_data_object('content', 'sign/preview/123', data)
    print s.get_meta('content', 'sign/preview/123')

