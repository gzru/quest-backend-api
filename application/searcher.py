from search_index import SearchIndex
import geo
import heapq
import logging
import math


class SearchEntry(object):

    def __init__(self, sign_id = 0, rank = 0, distance = 0):
        self.sign_id = sign_id
        self.rank = rank
        self.distance = distance


class SearchResultsHeap(object):

    def __init__(self, max_size, weight_fn = lambda x:x):
        self._pqueue = list()
        self._data = dict()
        self._max_size = max_size
        self._weight_fn = weight_fn

    def push(self, entry):
        if entry.sign_id in self._data:
            return

        weight = self._weight_fn(entry)
        if len(self._data) >= self._max_size:
            if self._pqueue[0][0] > weight:
                return
            self._data.pop(self._pqueue[0][1])
            heapq.heappop(self._pqueue)

        self._data[entry.sign_id] = entry
        heapq.heappush(self._pqueue, (weight, entry.sign_id))

    @property
    def results(self):
        return self._data


class SearchParams(object):

    SORT_BY_UNKNOWN = 0
    SORT_BY_RANK = 1
    SORT_BY_DISTANCE = 2

    def __init__(self):
        self.user_id = 0
        self.max_n = 100
        self.latitude = 0
        self.longitude = 0
        self.radius = 0
        self.min_rank = 0
        self.debug = False
        self.features = None
        self.sort_by = SearchParams.SORT_BY_UNKNOWN


class Searcher(object):

    def __init__(self, aerospike_connector):
        self._search_index = SearchIndex(aerospike_connector)

    def search(self, params):
        if params.features == None or len(params.features) == 0:
            signs = self._search_nearest(params)
        else:
            signs = self._search_by_fea(params)

        return self._make_result(params, signs)

    def _records_processor(self, params, heap):
        def _impl(record):
            entry = SearchEntry()
            entry.sign_id = int(record['sign_id'])
            entry.rank = record.get('rank')
            # Min rank threshold
            if entry.rank != None and entry.rank < params.min_rank:
                return
            # Legacy
            if entry.rank == None:
                entry.rank = 0

            location = record['location'].unwrap().get('coordinates')
            entry.distance = geo.distance(params.latitude, params.longitude, location[1], location[0])
            heap.push(entry)
        return _impl

    def _search_nearest(self, params):
        heap = SearchResultsHeap(params.max_n, lambda entry: -entry.distance)
        # Retrieve records
        self._search_index.search_nearest(params.user_id, \
                                          params.latitude, \
                                          params.longitude, \
                                          params.radius, \
                                          self._records_processor(params, heap))
        # Resort
        signs = heap.results.values()
        signs.sort(key=lambda entry: entry.distance)
        return signs

    def _search_by_fea(self, params):
        heap = SearchResultsHeap(params.max_n, lambda entry: entry.rank)
        # Normailize features vector
        qsum = 0
        for fea in params.features:
            qsum = qsum + fea * fea
        qsum = math.sqrt(qsum)
        for i in range(len(params.features)):
            params.features[i] = params.features[i] / qsum
        # Retrieve records
        self._search_index.search_by_fea(params.user_id, \
                                         params.latitude, \
                                         params.longitude, \
                                         params.radius, \
                                         params.features, \
                                         self._records_processor(params, heap))
        # Resort
        signs = heap.results.values()
        if params.sort_by == SearchParams.SORT_BY_RANK or \
           params.sort_by == SearchParams.SORT_BY_UNKNOWN:
            signs.sort(key=lambda entry: entry.rank, reverse=True)
        else:
            signs.sort(key=lambda entry: entry.distance)
        return signs

    def _make_result(self, params, signs):
        signs_list = list()
        for sign in signs:
            signs_list.append({
                'sign_id': sign.sign_id,
                'distance': sign.distance
            })

        debug = None
        if params.debug:
            weights = list()
            for sign in signs:
                weights.append({
                    'sign_id': sign.sign_id,
                    'weight': sign.rank
                })
            debug = {
                'weights': weights
            }
        return signs_list, debug


if __name__ == "__main__":
    import aerospike_connector

    connector = aerospike_connector.AerospikeConnector(128000, 1000, 1000)
    connector.connect(['localhost'])

    params = SearchParams()
    params.user_id = 10001
    params.latitude = 1
    params.longitude = 1.01
    params.radius = 10000
    params.debug = 1
    params.features = [1, 2, 3]

    searcher = Searcher(connector)
    result = searcher.search(params)
    print result

