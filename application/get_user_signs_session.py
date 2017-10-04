from query import Query, BadQuery
from users_engine import UsersEngine
from signs_engine import SignsEngine
import json
import logging


PRIVACY_PUBLIC = 1
PRIVACY_PRIVATE = 2
PRIVACY_ALL = 3

class GetUserSignsQuery(Query):

    def __init__(self):
        self.user_token = None
        self.user_id = None
        self.limit = None
        self.cursor = None
        self.privacy = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_token = self._get_required_str(tree, 'user_token')
        self.user_id = self._get_required_int64(tree, 'user_id')

        self.limit = self._get_optional_int64(tree, 'limit')
        if self.limit == None:
            self.limit = 100

        self.cursor = self._get_optional_str(tree, 'cursor')
        if self.cursor == None:
            self.cursor = ''

        privacy_str = self._get_optional_str(tree, 'privacy')
        if privacy_str == 'PUBLIC':
            self.privacy = PRIVACY_PUBLIC
        elif privacy_str == 'PRIVATE':
            self.privacy = PRIVACY_PRIVATE
        elif privacy_str == 'ALL':
            self.privacy = PRIVACY_ALL
        elif privacy_str == None:
            # default
            self.privacy = PRIVACY_PUBLIC
        else:
            raise BadQuery('Unknown privacy enum value')


class GetUserSignsSession(object):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)
        self._signs_enging = SignsEngine(global_context)

    def parse_query(self, data):
        self._query = GetUserSignsQuery()
        self._query.parse(data)

    def execute(self):
        page = self._users_engine.get_signs(self._query.user_id, self._query.limit, self._query.cursor)

        signs = list()
        # Retrieve aux data
        is_private_list = self._signs_enging.check_privacy_many(page.data)
        for i in range(len(page.data)):
            if is_private_list[i] == None:
                logging.warning('Check privacy for unknown sign {}'.format(page.data[i]))
                continue
            if (is_private_list[i] and self._query.privacy == PRIVACY_PUBLIC) \
                or (not is_private_list[i] and self._query.privacy == PRIVACY_PRIVATE):
                continue

            sign = {
                'sign_id': page.data[i],
                'is_private': is_private_list[i]
            }
            signs.append(sign)

        result = {
            'data': signs,
            'paging': { 'cursor': page.cursor_code, 'has_next': page.has_next }
        }
        return json.dumps(result)


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = GetUserSignsSession(global_context)
    s.parse_query('{ "user_token": "123", "user_id": 123, "limit": 1, "privacy": "PUBLIC" }')
    print s.execute()

