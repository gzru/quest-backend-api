from error import APILogicalError, APIInternalServicesError
from searcher_connector import SearcherMapClustersQParams
from search_index import SearchIndex
from searcher import Searcher, SearchParams
import hashlib
import requests
import logging
import json
import base64


class SignInfo(object):

    def __init__(self):
        self.sign_id = None
        self.user_id = None
        self.latitude = None
        self.longitude = None
        self.radius = None
        self.time_to_live = None
        self.timestamp = None
        self.is_private = True
        self.likes_count = 0
        self.views_count = 0

    def encode(self):
        # blob
        data = {
            'sign_id': self.sign_id,
            'user_id': self.user_id,
            'latitude': self.latitude,
            'longitude': self.longitude,
            'radius': self.radius,
            'time_to_live': self.time_to_live,
            'timestamp': self.timestamp
        }
        # a record itself
        record = {
            'data': json.dumps(data),
            'sign_id': self.sign_id,
            'user_id': self.user_id,
            'latitude': self.latitude,
            'longitude': self.longitude,
            'radius': self.radius,
            'time_to_live': self.time_to_live,
            'timestamp': self.timestamp,
            'is_private': int(self.is_private),
            'likes_count': self.likes_count,
            'views_count': self.views_count
        }
        return record

    def decode(self, record):
        # TODO: remove data json
        data = json.loads(record['data'])
        def _get(prop):
            return record.get(prop, data.get(prop))

        self.sign_id = _get('sign_id')
        self.user_id = _get('user_id')
        self.latitude = _get('latitude')
        self.longitude = _get('longitude')
        self.radius = _get('radius')
        self.time_to_live = _get('time_to_live')
        self.timestamp = int(_get('timestamp'))
        self.is_private = bool(record.get('is_private', True))
        self.likes_count = record.get('likes_count', 0)
        self.views_count = record.get('views_count', 0)


class SignsEngine(object):

    def __init__(self, global_context):
        self._aerospike_connector = global_context.aerospike_connector
        self._kafka_connector = global_context.kafka_connector
        self._searcher_connector = global_context.searcher_connector
        self._s3connector = global_context.s3connector
        self._search_index = SearchIndex(global_context.aerospike_connector)
        self._namespace = 'test'
        self._info_set = 'sign_info'
        self._features_set = 'sign_features'
        self._meta_set = 'sign_meta'
        self._object_set = 'sign_object'
        self._image_set = 'sign_image'
        self._image_thumb_set = 'sign_image_thumbnail'
        self._preview_set = 'sign_preview'
        self._access_set = 'relations_user_sign'
        self._searcher_host = '174.138.38.144'
        self._searcher_port = 28000
        self._searcher_req_timeout = 0.1

    def put_sign(self, info, features, meta_blob, object_blob, image_blob, preview_blob):
        # Generate sign id
        # It's convenient to detect duplicates
        info.sign_id = self._gen_sign_id(info, object_blob)
        # Check
        self._check_duplicate(info.sign_id)
        # Put data and then info
        self._search_index.add_sign(info, features)
        if meta_blob != None:
            self._put_meta(info.sign_id, meta_blob)
        if object_blob != None:
            self._put_object(info.sign_id, object_blob)
        if image_blob != None:
            self._put_image(info.sign_id, image_blob)
        if preview_blob != None:
            self._put_preview(info.sign_id, preview_blob)
        self._put_info(info.sign_id, info)
        # To searcher
        #self._put_to_searcher(info.sign_id, info, features)

        return info.sign_id

    def remove_sign(self, sign_id):
        self._aerospike_connector.remove((self._namespace, self._info_set, str(sign_id)))
        self._aerospike_connector.remove((self._namespace, self._preview_set, str(sign_id)))
        self._aerospike_connector.remove((self._namespace, self._image_set, str(sign_id)))
        self._aerospike_connector.remove((self._namespace, self._object_set, str(sign_id)))
        self._aerospike_connector.remove((self._namespace, self._meta_set, str(sign_id)))
        self._aerospike_connector.remove((self._namespace, self._features_set, str(sign_id)))
        self._search_index.remove_sign(sign_id)

    def check_privacy_many(self, sign_ids):
        keys = list()
        for sign_id in sign_ids:
            keys.append((self._namespace, self._info_set, str(sign_id)))
        info = self._aerospike_connector.get_bins_many(keys)

        result = list()
        for i in range(len(info)):
            entry = info[i]
            if entry == None:
                result.append(None)
            else:
                result.append(bool(entry.get('is_private', True)))
        return result

    def set_sign_privacy(self, sign_id, is_private):
        update = {
            'is_private': int(is_private)
        }
        self._aerospike_connector.put_bins((self._namespace, self._info_set, str(sign_id)), update)
        self._search_index.set_sign_privacy(sign_id, is_private)

    def grant_sign_access(self, user_id, sign_id):
        key = '{}:{}'.format(user_id, sign_id)
        data = {
            'data': '{}'
        }
        self._aerospike_connector.put_bins((self._namespace, self._access_set, key), data)
        self._search_index.add_private_access(user_id, sign_id)

    def _gen_sign_id(self, info, object_blob):
        hasher = hashlib.md5()
        def _update(value):
            if value:
                hasher.update(str(value))
        _update(info.user_id)
        _update(info.latitude)
        _update(info.longitude)
        _update(object_blob)
        return int(hasher.hexdigest()[:16], 16) & 0x7FFFFFFFFFFFFFFF

    def _check_duplicate(self, sign_id):
        info_key = (self._namespace, self._info_set, str(sign_id))
        if self._aerospike_connector.check_exists(info_key):
            raise APILogicalError('Sign already exists')

    def _put_info(self, sign_id, info):
        info_key = (self._namespace, self._info_set, str(sign_id))
        self._aerospike_connector.put_bins(info_key, info.encode())

    def get_info(self, sign_id):
        info_key = (self._namespace, self._info_set, str(sign_id))

        record = self._aerospike_connector.get_bins(info_key)
        if record == None:
            raise APILogicalError('Sign {} not found'.format(sign_id))

        try:
            info = SignInfo()
            info.decode(record)
        except Exception as ex:
            logging.error('Bad info record, {}'.format(ex))
            raise APILogicalError('Bad info record, {}'.format(ex))
        return info

    def get_info_many(self, sign_ids):
        keys = list()
        for sign_id in sign_ids:
            keys.append((self._namespace, self._info_set, str(sign_id)))
        records = self._aerospike_connector.get_bins_many(keys)

        result = list()
        for record in records:
            if record == None:
                result.append(None)
            else:
                info = SignInfo()
                info.decode(record)
                result.append(info)
        return result

    def _put_features(self, sign_id, features):
        info_key = (self._namespace, self._features_set, str(sign_id))
        data = {
            'sign_id': sign_id,
            'features': features
        }
        self._aerospike_connector.put_bins(info_key, {'data': json.dumps(data)})

    def _put_meta(self, sign_id, meta_blob):
        self._aerospike_connector.put_data((self._namespace, self._meta_set, str(sign_id)), meta_blob)

    def get_meta(self, sign_id):
        return self._get_blob((self._namespace, self._meta_set, str(sign_id)))

    def _put_object(self, sign_id, object_blob):
        self._aerospike_connector.put_data((self._namespace, self._object_set, str(sign_id)), object_blob)

    def get_object(self, sign_id):
        return self._get_blob((self._namespace, self._object_set, str(sign_id)))

    def _put_image(self, sign_id, image_blob):
        self._aerospike_connector.put_data((self._namespace, self._image_set, str(sign_id)), image_blob)
        self._s3connector.put_data_object('content', 'sign/{}/background'.format(sign_id), image_blob)

    def get_image(self, sign_id):
        return self._get_blob((self._namespace, self._image_set, str(sign_id)))

    def _put_preview(self, sign_id, preview_blob):
        self._aerospike_connector.put_data((self._namespace, self._preview_set, str(sign_id)), preview_blob)
        self._s3connector.put_data_object('content', 'sign/{}/preview'.format(sign_id), preview_blob)

    def get_preview(self, sign_id):
        return self._get_blob((self._namespace, self._preview_set, str(sign_id)))

    def _get_blob(self, key):
        data = self._aerospike_connector.get_data(key)
        if data == None:
            return None
        return base64.b64encode(data)

    def _put_to_searcher(self, sign_id, info, features):
        try:
            request = {
                'sign_id': sign_id,
                'user_id': info.user_id,
                'latitude': info.latitude,
                'longitude': info.longitude,
                'radius': info.radius,
                'time_to_live': info.time_to_live,
                'timestamp': info.timestamp,
                'features': features
            }
            url = 'http://{}:{}/api/sign/put'.format(self._searcher_host, self._searcher_port)
            resp = requests.post(url, data=json.dumps(request), timeout=self._searcher_req_timeout)
            result = json.loads(resp.text)
        except Exception as ex:
            logging.error('Searcher put request failed: {}'.format(ex))
            raise APIInternalServicesError('Searcher put request failed: {}'.format(ex))

        if result.get('sign_id') == None:
            raise APIInternalServicesError('Searcher put request failed: Bad response: {}'.format(resp.text))

    def search_signs(self, user_id, lat, lon, radius, max_n, min_rank, sort_by, debug, features):
        signs, debug = self._ask_searcher(user_id, lat, lon, radius, max_n, min_rank, sort_by, debug, features)
        sign_ids = list()
        for sign in signs:
            sign_ids.append(sign['sign_id'])

        # TODO
        info_list = self.get_info_many(sign_ids)
        has_access_list = self._check_access(user_id, sign_ids)

        result = list()
        for i in range(len(signs)):
            info = info_list[i]
            if info == None:
                logging.warning('Found removed sign or logical error, sign_id = {}'.format(sign_ids[i]))
                continue
            if info.is_private and info.user_id != user_id and not has_access_list[i]:
                continue
            result.append(signs[i])
        return result, debug

    def get_signs_clusters(self, params):
        return self._searcher_connector.get_map_clusters(params)

    def _ask_searcher(self, user_id, lat, lon, radius, max_n, min_rank, sort_by, debug, features):
        logging.info('_ask_searcher: Retrieve signs')
        try:
            request = {
                'user_id': user_id,
                'latitude': lat,
                'longitude': lon,
                'radius': radius,
                'max_n': max_n,
                'min_rank': min_rank,
                'features': features
            }
            if sort_by != None:
                request['sort_by'] = sort_by
            if debug != None:
                request['debug'] = debug

            url = 'http://{}:{}/api/sign/search'.format(self._searcher_host, self._searcher_port)
            resp = requests.post(url, data=json.dumps(request), timeout=self._searcher_req_timeout)
            result = json.loads(resp.text)
        except Exception as ex:
            logging.error('Searcher search request failed: {}'.format(ex))
            raise APIInternalServicesError('Searcher search request failed: {}'.format(ex))

        signs = result.get('signs')
        if signs == None:
            raise APIInternalServicesError('Searcher search request failed: Bad response: {}'.format(resp.text))

        debug = result.get('debug')

        logging.info('_ask_searcher: Got {} signs'.format(len(signs)))
        return signs, debug

    def _check_access(self, user_id, sign_ids):
        keys = list()
        for sign_id in sign_ids:
            key = '{}:{}'.format(user_id, sign_id)
            keys.append((self._namespace, self._access_set, key))
        return self._aerospike_connector.check_exists_many(keys)

    def add_sign_like(self, user_id, sign_id):
        key = '{}:{}'.format(user_id, sign_id)
        record = self._aerospike_connector.get_bins((self._namespace, self._access_set, key))
        if record != None and 'like' in record:
            raise APILogicalError('Sign have been liked already')

        data = {
            'like': '1'
        }
        self._aerospike_connector.put_bins((self._namespace, self._access_set, key), data)
        self._aerospike_connector.increment((self._namespace, self._info_set, str(sign_id)), 'likes_count')

    def add_sign_view(self, user_id, sign_id):
        key = '{}:{}'.format(user_id, sign_id)
        record = self._aerospike_connector.get_bins((self._namespace, self._access_set, key))
        if record != None and 'view' in record:
            raise APILogicalError('Sign have been viewed already')

        data = {
            'view': '1'
        }
        self._aerospike_connector.put_bins((self._namespace, self._access_set, key), data)
        self._aerospike_connector.increment((self._namespace, self._info_set, str(sign_id)), 'views_count')

    def remove_sign_like(self, user_id, sign_id):
        key = '{}:{}'.format(user_id, sign_id)
        record = self._aerospike_connector.get_bins((self._namespace, self._access_set, key))
        if record == None or 'like' not in record:
            raise APILogicalError('Sign is not liked yet')

        self._aerospike_connector.remove_bin((self._namespace, self._access_set, key), ['like'])
        self._aerospike_connector.increment((self._namespace, self._info_set, str(sign_id)), 'likes_count', -1)

    def remove_sign_view(self, user_id, sign_id):
        key = '{}:{}'.format(user_id, sign_id)
        record = self._aerospike_connector.get_bins((self._namespace, self._access_set, key))
        if record == None or 'view' not in record:
            raise APILogicalError('Sign is not viewed yet')

        self._aerospike_connector.remove_bin((self._namespace, self._access_set, key), ['view'])
        self._aerospike_connector.increment((self._namespace, self._info_set, str(sign_id)), 'views_count', -1)

    def check_likes(self, user_id, sign_ids):
        keys = list()
        for sign_id in sign_ids:
            key = '{}:{}'.format(user_id, sign_id)
            keys.append((self._namespace, self._access_set, key))
        records = self._aerospike_connector.get_bins_many(keys)
        result = list()
        for record in records:
            if record == None or 'like' not in record:
                result.append(False)
            else:
                result.append(True)
        return result

    def check_views(self, user_id, sign_ids):
        keys = list()
        for sign_id in sign_ids:
            key = '{}:{}'.format(user_id, sign_id)
            keys.append((self._namespace, self._access_set, key))
        records = self._aerospike_connector.get_bins_many(keys)
        result = list()
        for record in records:
            if record == None or 'view' not in record:
                result.append(False)
            else:
                result.append(True)
        return result


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    info = SignInfo()
    info.sign_id = 123
    info.user_id = 123
    info.latitude = 1
    info.longitude = 2
    info.radius = 100
    info.time_to_live = 0
    info.timestamp = 0

    features = [1, 2, 3]

    se = SignsEngine(global_context)
    se.put_sign(info, features, None, None, None, None)
    #se.remove_sign_like(123, 5435010004366254830)
    #print se.check_likes(5435010004366254830, [345, 567])
    #print se.get_info(5435010004366254830).likes_count

    #info.sign_id = se._gen_sign_id(info, object_blob)

    #print se.put_sign(info, features, meta_blob, object_blob, image_blob, preview_blob)
    #se.set_sign_privacy(5136828351633214532, True)
    #print se.check_privacy_many([5136828351633214532])

