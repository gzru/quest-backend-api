from error import APIInternalServicesError, APILogicalError
import aerospike
import logging


class AerospikeConnector:
    def __init__(self, max_record_size, rw_timeout_ms, connection_timeout_ms):
        self._client = None
        self._max_record_size = max_record_size
        self._connection_timeout_ms = connection_timeout_ms
        self._read_policy = {
            'timeout': rw_timeout_ms
        }
        self._write_policy = {
            'timeout': rw_timeout_ms,
            'key': aerospike.POLICY_KEY_SEND
        }
        self._scan_policy = {
            'timeout': rw_timeout_ms
        }

    def connect(self, hosts):
        config = {
            'hosts': hosts,
            'policies': {
                'timeout': self._connection_timeout_ms
            }
        }

        try:
            self._client = aerospike.client(config)
            self._client.connect()
        except Exception as ex:
            logging.error('Database error: {}'. format(ex))
            return False
        return True

    def check_exists(self, key):
        try:
            (_, meta) = self._client.exists(key, policy=self._read_policy)
            if meta == None:
                return False
            return True
        except Exception as ex:
            logging.error('Database error: {}'.format(ex))
            raise APIInternalServicesError('Database error: {}'.format(ex))

    def check_exists_many(self, keys):
        result = list()
        try:
            response = self._client.exists_many(keys, policy=self._read_policy)
            for entry in response:
                if entry[1] == None:
                    result.append(False)
                else:
                    result.append(True)
        except Exception as ex:
            logging.error('Database error: {}'.format(ex))
            raise APIInternalServicesError('Database error: {}'.format(ex))
        return result

    def remove(self, key):
        try:
            self._client.remove(key, policy=self._write_policy)
        except aerospike.exception.RecordNotFound:
            pass
        except Exception as ex:
            logging.error('Database error: {}'.format(ex))
            raise APIInternalServicesError('Database error: {}'.format(ex))

    def remove_bin(self, key, bins):
        try:
            self._client.remove_bin(key, bins, policy=self._write_policy)
        except aerospike.exception.RecordNotFound:
            # May be it's error
            pass
        except Exception as ex:
            logging.error('Database error: {}'.format(ex))
            raise APIInternalServicesError('Database error: {}'.format(ex))

    def put_bins(self, key, bins, ttl=0):
        self._put_one(key, bins, ttl)

    def get_bins(self, key):
        return self._get_one(key)

    def get_bins_many(self, keys):
        return self._get_many(keys)

    def increment(self, key, bin, value=1):
        try:
            self._client.increment(key, bin, value, policy=self._write_policy)
        except Exception as ex:
            logging.error('Database error: {}'.format(ex))
            raise APIInternalServicesError('Database error: {}'.format(ex))

    def put_data(self, key, data, ttl=0):
        chunks = self._split_by_chunks(key, data)
        for chunk in chunks:
            bins = {
                'data': bytearray(chunk[1]),
                'chunks_count': len(chunks)
            }
            self._put_one(chunk[0], bins, ttl)
        return True

    def get_data(self, key):
        bins = self._get_one(key)
        if bins == None:
            return None

        data = bins.get('data')
        if data == None:
            logging.error('Missed data bin')
            raise APILogicalError('Missed data bin')

        chunks_count = bins.get('chunks_count')
        if not chunks_count or chunks_count == 1:
            return bins.get('data')

        result = bytearray(data)
        chunk_keys = list()
        for i in range(1, chunks_count):
            chunk_keys.append(self._chunk_subkey(key, i))

        chunks = self._get_many(chunk_keys)
        for chunk in chunks:
            if not chunk:
                logging.error('One of data chunks missed')
                raise APILogicalError('One of data chunks missed')
            data = chunk.get('data')
            if not data:
                logging.error('Missed data bin')
                raise APILogicalError('Missed data bin')
            result.extend(data)
        return result

    def scan(self, namespace, set, bins):
        try:
            scan = self._client.scan(namespace, set)
            scan.select(*bins)
            response = scan.results(policy=self._scan_policy)
            return [entry[2] for entry in response]
        except Exception as ex:
            logging.error('Database error: {}'.format(ex))
            raise APIInternalServicesError('Database error: {}'.format(ex))

    def list_append(self, key, bin, val):
        try: 
            self._client.list_append(key, bin, val, policy=self._write_policy)
        except Exception as ex:
            logging.error('Database error: {}'.format(ex))
            raise APIInternalServicesError('Database error: {}'.format(ex))

    def list_get_range(self, key, bin, index, count):
        try: 
            return self._client.list_get_range(key, bin, index, count, policy=self._read_policy)
        except aerospike.exception.RecordNotFound:
            return None
        except Exception as ex:
            logging.error('Database error: {}'.format(ex))
            raise APIInternalServicesError('Database error: {}'.format(ex))

    def list_size(self, key, bin):
        try: 
            return self._client.list_size(key, bin, policy=self._read_policy)
        except aerospike.exception.RecordNotFound:
            return 0
        except Exception as ex:
            logging.error('Database error: {}'.format(ex))
            raise APIInternalServicesError('Database error: {}'.format(ex))

    def list_remove(self, key, bin, index):
        try:
            return self._client.list_remove(key, bin, index, policy=self._write_policy)
        except aerospike.exception.RecordNotFound:
            return
        except Exception as ex:
            logging.error('Database error: {}'.format(ex))
            raise APIInternalServicesError('Database error: {}'.format(ex))

    def _split_by_chunks(self, key, data):
        chunk_size = self._max_record_size
        chunks_count = int((len(data) + chunk_size - 1) / chunk_size)
        chunks = list()
        if chunks_count == 1:
            chunks.append((key, data))
        else:
            chunks.append((key, data[0:chunk_size]))
            for i in range(1, chunks_count):
                subkey = self._chunk_subkey(key, i)
                begin = i * chunk_size
                end = min((i + 1) * chunk_size, len(data))
                chunks.append((subkey, data[begin:end]))
        return chunks

    def _chunk_subkey(self, key, n):
        return (key[0], key[1], '{}_{}'.format(key[2], n))

    def _get_one(self, key):
        try:
            (_, _, bins) = self._client.get(key, policy=self._read_policy)
        except aerospike.exception.RecordNotFound:
            return None
        except Exception as ex:
            logging.error('Database error: {}'.format(ex))
            raise APIInternalServicesError('Database error: {}'.format(ex))
        return bins

    def _get_many(self, keys):
        result = list()
        try:
            response = self._client.get_many(keys, policy=self._read_policy)
            for entry in response:
                result.append(entry[2])
        except Exception as ex:
            logging.error('Database error: {}'.format(ex))
            raise APIInternalServicesError('Database error: {}'.format(ex))
        return result

    def _put_one(self, key, bins, ttl):
        try: 
            self._client.put(key, bins, meta={'ttl': ttl}, policy=self._write_policy)
        except Exception as ex:
            logging.error('Database error: {}'.format(ex))
            raise APIInternalServicesError('Database error: {}'.format(ex))

