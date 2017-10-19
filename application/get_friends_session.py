from query import Query
from profile_session_base import UserInfo, ProfileSessionBase
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


class GetFriendsSession(ProfileSessionBase):

    def parse_query(self, data):
        self._query = GetFriendsQuery()
        self._query.parse(data)

    def execute(self):
        page = self._engine.get_friends(self._query.user_id, self._query.limit, self._query.cursor)
        relations = self._engine.get_relations_many(self._query.user_id, page.data)

        friends = list()
        # Retrieve aux data
        for i, user_id in enumerate(page.data):
            if not relations[i].is_friends:
                continue

            profile = dict()
            # fix me, use group request
            info = None
            try:
                info = self._get_info(user_id)
            except:
                pass
            if info == None:
                logging.warning('Can\'t find user profile, id = {}'.format(user_id))
                continue

            profile['user_id'] = user_id
            profile['name'] = info.get('name')
            profile['twilio_channel'] = relations[i].twilio_channel

            friends.append(profile)

        result = {
            'success': True,
            'data': friends,
            'paging': { 'cursor': page.cursor_code, 'has_next': page.has_next }
        }
        return json.dumps(result)

"""
from global_context import GlobalContext
global_context = GlobalContext()
global_context.initialize()

s = GetFriendsSession(global_context)
s.parse_query('{ "user_id": 123, "limit": 1 }')
print s.execute()

s.parse_query('{ "user_id": 123, "limit": 2, "cursor": "eyJiIjogMCwgIm8iOiAxfQ==" }')
print s.execute()
"""

