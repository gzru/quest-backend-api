from query import Query, BadQuery
from twilio.jwt.access_token import AccessToken
from twilio.jwt.access_token.grants import ChatGrant
import json


class MakeMessagesTokenQuery(Query):

    def __init__(self):
        self.user_id = None
        self.uuid = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_id = self._get_required_int64(tree, 'user_id')
        self.sign_id = self._get_required_str(tree, 'uuid')


class MakeMessagesTokenSession:

    def __init__(self, global_context):
        self._account_sid = 'AC5ad3dea7a74001036c0ed179284a5965'
        self._api_key = 'SK9cf74c45159a16016d9c88c142b2eb50'
        self._api_secret = 'Ysg3PJ3Mg1szHNvYX258DTiMUKqBt1Gz'
        self._chat_service_sid = 'ISe5fee5b44dd44918b6c8fc615e90b3c9'

    def parse_query(self, data):
        self._query = MakeMessagesTokenQuery()
        self._query.parse(data)

    def execute(self):
        identity = '{}_{}'.format(self._query.user_id, self._query.uuid)

        # Create access token with credentials
        token = AccessToken(self._account_sid, self._api_key, self._api_secret, identity=identity)

        # Create an Chat grant and add to token
        chat_grant = ChatGrant(service_sid=self._chat_service_sid)
        token.add_grant(chat_grant)

        result = { 'token': token.to_jwt().decode('utf-8') }
        return json.dumps(result)

