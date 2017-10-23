from query import Query
from profile_session_base import UserInfo, ProfileSessionBase
from error import APILogicalError
import json
import requests
import logging


class GetExternalFriendsQuery(Query):

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


class GetExternalFriendsSession(ProfileSessionBase):

    def parse_query(self, data):
        self._query = GetExternalFriendsQuery()
        self._query.parse(data)

    def execute(self):
        info = self._engine.get_info(self._query.user_id)
        if info == None:
            raise APILogicalError('User {} not found'.format(self._query.user_id))

        facebook_user_id = info.facebook_user_id
        facebook_access_token = info.facebook_access_token

        friends = list()

        page_uri = 'https://graph.facebook.com/{}/friends?access_token={}&limit={}&after={}'.format(facebook_user_id, facebook_access_token, self._query.limit, self._query.cursor)
        try:
            resp = requests.get(page_uri)
            # parse
            parsed = json.loads(resp.text)
            # check errors
            error = parsed.get('error')
            if error != None:
                logging.error('Facebook error: {}'.format(error.get('message')))
                raise Exception('Can\'t get data from Facebook')
            # take profiles
            data = parsed.get('data')

            profiles = list()
            external_ids = list()
            for entry in data:
                profile = {
                    'facebook_user_id': entry.get('id'),
                    'name': entry.get('name')
                }
                profiles.append(profile)
                external_ids.append(entry.get('id'))

            local_ids = self._engine.external_to_local_id_many(external_ids)
            for i in range(len(local_ids) - 1, -1, -1):
                if local_ids[i] == None:
                    del local_ids[i]
                    del profiles[i]
                profiles[i]['user_id'] = local_ids[i]

            check_result = self._engine.check_friends_many(self._query.user_id, local_ids)
            for i in range(len(check_result)):
                if not check_result[i]:
                    friends.append(profiles[i])

            # paging
            cursor, has_next = self._parse_paging(parsed.get('paging'))
        except Exception as ex:
            logging.error(ex)
            raise

        result = {
            'success': True,
            'data': friends,
            'paging': { 'cursor': cursor, 'has_next': has_next }
        }

        return json.dumps(result)

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


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = GetExternalFriendsSession(global_context)
    s.parse_query('{"user_token": "8618994807331250316", "user_id": 8618994807331250316}')
    print s.execute()

