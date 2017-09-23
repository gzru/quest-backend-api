import aerospike


class AerospikeConnector:
    def __init__(self):
        self._client = None
        self._max_record_size = 128000
        self._rw_timeout = 500
        self._read_policy = {
            'timeout': self._rw_timeout
        }
        self._write_policy = {
            'timeout': self._rw_timeout
        }

    def connect(self, hosts):
        conf_hosts = list()
        for host in hosts:
            conf_hosts.append((host, 3000))

        config = {
            'hosts': conf_hosts,
            'policies': {
                'timeout': 1000
            }
        }

        try:
            self._client = aerospike.client(config)
            self._client.connect()
        except Exception as e:
            # log error
            return False
        return True

    def check_exists(self, key):
        (_, meta) = self._client.exists(key, policy=self._read_policy)
        if not meta:
            return False
        return True

    def put_bins(self, key, bins, ttl=0):
        return self._put_one(key, bins, ttl)

    def get_bins(self, key):
        return self._get_one(key)

    def put_data(self, key, data, ttl=0):
        chunks = self._split_by_chunks(key, data)
        for chunk in chunks:
            bins = {
                'data': bytearray(chunk[1]),
                'chunks_count': len(chunks)
            }
            if not self._put_one(chunk[0], bins, ttl):
                return False
        return True

    def get_data(self, key):
        bins = self._get_one(key)
        if not bins:
            return None

        data = bins.get('data')
        if not data:
            # log error
            return None

        chunks_count = bins.get('chunks_count')
        if not chunks_count or chunks_count == 1:
            return bins.get('data')

        result = bytearray(data)
        chunk_keys = list()
        for i in range(1, chunks_count):
            chunk_keys.append(self._chunk_subkey(key, i))

        chunks = self._get_many(chunk_keys)
        if not chunks:
            # log error
            return None

        for chunk in chunks:
            if not chunk:
                # log error
                return None
            data = chunk.get('data')
            if not data:
                # log error
                return None
            result.extend(data)
        return result

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
        except Exception as ex:
            return None
        return bins

    def _get_many(self, keys):
        result = list()
        try:
            response = self._client.get_many(keys, policy=self._read_policy)
            for entry in response:
                result.append(entry[2])
        except Exception as ex:
            # log error
            return None
        return result

    def _put_one(self, key, bins, ttl):
        try: 
            self._client.put(key, bins, meta={'ttl': ttl}, policy=self._write_policy)
        except Exception as ex:
            # log error
            return False
        return True

