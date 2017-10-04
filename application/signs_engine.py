from error import APILogicalError, APIInternalServicesError
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
            'is_private': int(self.is_private)
        }
        return record

    def decode(self, record):
        # blob
        data = json.loads(record['data'])
        self.sign_id = data['sign_id']
        self.user_id = data['user_id']
        self.latitude = data['latitude']
        self.longitude = data['longitude']
        self.radius = data['radius']
        self.time_to_live = data['time_to_live']
        self.timestamp = int(data['timestamp'])
        # rest
        self.is_private = bool(record.get('is_private', True))


class SignsEngine(object):

    def __init__(self, global_context):
        self._aerospike_connector = global_context.aerospike_connector
        self._kafka_connector = global_context.kafka_connector
        self._namespace = 'test'
        self._info_set = 'sign_info'
        self._features_set = 'sign_features'
        self._meta_set = 'sign_meta'
        self._object_set = 'sign_object'
        self._image_set = 'sign_image'
        self._image_thumb_set = 'sign_image_thumbnail'
        self._preview_set = 'sign_preview'
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
        self._put_features(info.sign_id, features)
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
        self._put_to_searcher(info.sign_id, info, features)

        return info.sign_id

    def remove_sign(self, sign_id):
        self._aerospike_connector.remove((self._namespace, self._info_set, str(sign_id)))
        self._aerospike_connector.remove((self._namespace, self._preview_set, str(sign_id)))
        self._aerospike_connector.remove((self._namespace, self._image_set, str(sign_id)))
        self._aerospike_connector.remove((self._namespace, self._object_set, str(sign_id)))
        self._aerospike_connector.remove((self._namespace, self._meta_set, str(sign_id)))
        self._aerospike_connector.remove((self._namespace, self._features_set, str(sign_id)))

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

    def get_image(self, sign_id):
        return self._get_blob((self._namespace, self._image_set, str(sign_id)))

    def _put_preview(self, sign_id, preview_blob):
        self._aerospike_connector.put_data((self._namespace, self._preview_set, str(sign_id)), preview_blob)

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


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    se = SignsEngine(global_context)

    info = SignInfo()
    info.user_id = 123
    info.latitude = 1;
    info.longitude = 2;
    info.radius = 100;

    features = [1, 2, 3]
    meta_blob = 'a'
    object_blob = 'ab'
    image_blob = 'abc'
    preview_blob = 'abcd'

    #info.sign_id = se._gen_sign_id(info, object_blob)

    #print se.put_sign(info, features, meta_blob, object_blob, image_blob, preview_blob)
    #se.set_sign_privacy(5136828351633214532, True)
    #print se.check_privacy_many([5136828351633214532])

