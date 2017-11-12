from query import Query
from users_engine import UsersEngine
import json
import logging


class GetFriendsQuery(Query):

    def __init__(self):
        self.user_token = None
        self.user_id = None
        self.limit = None
        self.cursor = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_token = self._get_required_str(tree, 'user_token')
        self.user_id = self._get_optional_int64(tree, 'user_id')
        if self.user_id == None:
            self.user_id = int(self.user_token)

        self.limit = self._get_optional_int64(tree, 'limit')
        if self.limit == None:
            self.limit = 100

        self.cursor = self._get_optional_str(tree, 'cursor')
        if self.cursor == None:
            self.cursor = ''


class GetFriendsSession(object):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)

    def parse_query(self, data):
        self._query = GetFriendsQuery()
        self._query.parse(data)

    def execute(self):
        page = self._users_engine.get_friends(self._query.user_id, self._query.limit, self._query.cursor)
        relations = self._users_engine.get_relations_many(self._query.user_id, page.data)

        friends = list()
        # Retrieve aux data
        for i, user_id in enumerate(page.data):
            if relations[i] == None or not relations[i].is_friends:
                continue

            # fix me, use group request
            info = None
            try:
                info = self._users_engine.get_info(user_id)
            except:
                pass
            if info == None:
                logging.warning('Can\'t find user profile, id = {}'.format(user_id))
                continue

            profile = {
                'user_id': info.user_id,
                'facebook_user_id': info.facebook_user_id,
                'name': info.name,
                'username': info.username,
                'email': info.email,
                'twilio_channel': relations[i].twilio_channel
            }
            friends.append(profile)

        result = {
            'success': True,
            'data': friends,
            'paging': { 'cursor': page.cursor_code, 'has_next': page.has_next }
        }
        return json.dumps(result)


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = GetFriendsSession(global_context)
    s.parse_query('{"user_token": "8618994807331250316", "user_id": 8618994807331250316, "properties": ["friends"]}')
    print s.execute()

