from query import Query
import json


class MakeMessagingTokenQuery(Query):

    def __init__(self):
        self.user_id = None
        self.uuid = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_id = self._get_required_int64(tree, 'user_id')
        self.sign_id = self._get_required_str(tree, 'uuid')


class MakeMessagingTokenSession(object):

    def __init__(self, global_context):
        self._twilio_connector = global_context.twilio_connector

    def parse_query(self, data):
        self._query = MakeMessagingTokenQuery()
        self._query.parse(data)

    def execute(self):
        token = self._twilio_connector.make_messaging_token(self._query.user_id, self._query.uuid)
        result = {
            'success': True,
            'data': {
                'token': token
            }
        }
        return json.dumps(result)

