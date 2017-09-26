from query import Query
from profile_session_base import UserInfo, ProfileSessionBase
import json
import requests
import logging


class GetExternalFriendsQuery(Query):

    def __init__(self):
        self.user_id = None
        self.limit = None
        self.cursor = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_id = self._get_required_int64(tree, 'user_id')

        self.limit = self._get_optional_int64(tree, 'limit')
        if self.limit == None:
            self.limit = 100

        self.cursor = self._get_optional_str(tree, 'cursor')
        if self.cursor == None:
            self.cursor = ''


class GetExternalFriendsSession(ProfileSessionBase):

    def parse_query(self, data):
        self._query = GetExternalFriendsQuery()
        self._query.parse(data)

    def execute(self):
        info = self._get_info(self._query.user_id)
        facebook_user_id = info['facebook_user_id']
        facebook_access_token = info['facebook_access_token']

        friends = list()

        page_uri = 'https://graph.facebook.com/{}/friends?access_token={}&limit={}&after={}'.format(facebook_user_id, facebook_access_token, self._query.limit, self._query.cursor)
        try:
            resp = requests.get(page_uri)
            # parse
            parsed = json.loads(resp.text)
            # take profiles
            data = parsed.get('data')
            for entry in data:
                facebook_user_id = entry.get('id')

                profile = dict()
                profile['facebook_user_id'] = facebook_user_id
                profile['user_id'] = self._get_user_id_by_fb(facebook_user_id)
                profile['name'] = entry.get('name')

                friends.append(profile)
            # paging
            cursor, has_next = self._parse_paging(parsed.get('paging'))
        except Exception as ex:
            logging.error(ex)
            raise

        result = {
            'data': friends,
            'paging': { 'cursor': cursor, 'has_next': has_next }
        }

        return json.dumps(result)

    def _get_user_id_by_fb(self, facebook_user_id):
        user_id = self._get_external_link(facebook_user_id)
        if not user_id:
            logging.warning('Can\'t find user by facebook user id')

            info = UserInfo()
            info.facebook_user_id = facebook_user_id

            # Generate user id
            info.user_id = self._gen_user_id(info)

            # Create profile
            self._put_info(info.user_id, info)
            self._put_external_link(info.user_id, facebook_user_id)

        return int(user_id)

    def _parse_paging(self, paging):
        cursor = ''
        has_next = False

        if paging == None:
            return (cursor, has_next)

        cursors = paging.get('cursors')
        if cursors != None:
            cursor = cursors.get('after', '')

        if 'next' in paging:
            has_next = True

        return (cursor, has_next)

"""
from global_context import GlobalContext
global_context = GlobalContext()
global_context.initialize()

s = GetExternalFriendsSession(global_context)
s.parse_query('{ "user_id": 123, "limit": 1 }')
print s.execute()

s.parse_query('{ "user_id": 123, "limit": 2, "cursor": "QVFIUllzd0dQdDdrcWd5eWFiWWtCQ0dqQmsxSXVyeUhycGlnNnQwTDk3bEFTb1JUeUNOMEgyNzU3QzZAvNjVIelBZAWHYZD" }')
print s.execute()
"""

