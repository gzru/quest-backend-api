from query import Query, BadQuery
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


class GetMatchingSession:

    def __init__(self, global_context):
        self._matcher_host = '162.243.160.162'
        self._matcher_api_handler = '/api/matching/get'
        self._timeout_sec = 10

    def parse_query(self, data):
        self._query = GetMatchingQuery()
        self._query.parse(data)

    def execute(self):
        results = self._send_requests(self._gen_matching_requests(self._query))
        return json.dumps({ 'matches': results })

    def _gen_matching_requests(self, query):
        request = dict()
        request['image_screen'] = base64.b64encode(query.screen_blob)

        for sign_id in query.signs:
            request['sign_id'] = sign_id
            yield (sign_id, json.dumps(request))

    def _send_requests(self, requests_gen):
        def _request_wrapper((sign_id, request)):
            try:
                url = 'http://' + self._matcher_host + self._matcher_api_handler
                resp = requests.post(url, data=request, timeout=self._timeout_sec)
            except Exception as ex:
                logging.error(ex)
                return { 'sign_id': sign_id, 'error': 'matching/get request failed' }

            try:
                return json.loads(resp.text)
            except Exception as ex:
                # log
                return { 'sign_id': sign_id, 'error': 'bad response' }

        pool = multiprocessing.dummy.Pool(5)
        result = pool.map(_request_wrapper, requests_gen)
        pool.close()
        return result

