from query import Query, BadQuery
from error import APIInternalServicesError
from PIL import Image
from io import BytesIO
import json
import base64
import multiprocessing.dummy
import requests
import logging


class GetMatchingQuery(Query):

    def __init__(self):
        self.user_id = None
        self.signs = None
        self.screen_blob = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_id = self._get_optional_int64(tree, 'user_id')
        self.signs = self._get_signs(tree)
        self.screen_blob = self._get_required_blob(tree, 'image_screen')

    def _get_signs(self, tree):
        signs = tree.get('signs')
        if not signs:
            raise BadQuery('have no "signs"')

        if not isinstance(signs, list):
            raise BadQuery('"signs" have bad format')

        for sign_id in signs:
            self._check_float('sign_id', sign_id)

        return signs


class GetMatchingSession(object):

    def __init__(self, global_context):
        self._aerospike_connector = global_context.aerospike_connector
        self._namespace = 'test'
        self._image_set = 'sign_image'
        self._matcher_host = '162.243.160.162'
        self._matcher_api_handler = '/api/matching/get'
        self._timeout_sec = 10

    def parse_query(self, data):
        self._query = GetMatchingQuery()
        self._query.parse(data)

    def execute(self):
        logging.info('Get matchings for {} signs'.format(len(self._query.signs)))
        results = self._send_requests(self._gen_matching_requests(self._query))
        return json.dumps({ 'matches': results })

    def _gen_matching_requests(self, query):
        image_screen_b64 = self._prepare_image_data(query.screen_blob)
        for sign_id in query.signs:
            yield (sign_id, image_screen_b64)

    def _prepare_image_data(self, data):
        buf = BytesIO()
        img = Image.open(BytesIO(data))

        logging.info('Prepare image, base size {}'.format(img.size))

        img.thumbnail((400, 400))
        img.save(buf, format='JPEG')
        return base64.b64encode(buf.getvalue())

    def _send_one_request(self, (sign_id, image_screen_b64)):
        # get
        image_background_data = self._aerospike_connector.get_data((self._namespace, self._image_set, str(sign_id)))
        if image_background_data == None:
            logging.warning('Background image not found, sing_id = {}'.format(sign_id))
            return { 'error': 'Background image not found' }
        image_background_b64 = self._prepare_image_data(image_background_data)
        # send query
        try:
            request = json.dumps({'image_screen': image_screen_b64, 'image_background': image_background_b64})

            url = 'http://' + self._matcher_host + self._matcher_api_handler
            resp = requests.post(url, data=request, timeout=self._timeout_sec)
        except Exception as ex:
            logging.error(ex)
            raise APIInternalServicesError('matching/get request failed')
        # parse response
        try:
            result = {
                'sign_id': sign_id,
                'transformation': json.loads(resp.text)
            }
            return result
        except Exception as ex:
            logging.error('{}\n{}'.format(ex, resp.text[:100]))
            raise APIInternalServicesError('matching/get request failed, bad response')

    def _send_requests(self, requests_gen):
        pool = multiprocessing.dummy.Pool(5)
        result = pool.map(self._send_one_request, requests_gen)
        pool.close()
        return result


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    with open('/tmp/test_sign.jpg', 'r') as myfile:
        data = base64.b64encode(myfile.read())
    query = {
        'signs': [1, 2, 3],
        'image_screen': data
    }

    s = GetMatchingSession(global_context)
    s._prepare_image_data(base64.b64decode(data))
    #s._send_one_request((1, data))
    #s.parse_query(json.dumps(query))
    #print s.execute()

