from sessions.session import POSTSession
from users_engine import UsersEngine
from error import APILogicalError
import requests
import logging


class Params(object):

    def __init__(self):
        self.user_token = None
        self.user_id = None
        self.limit = None
        self.cursor = None

    def parse(self, query):
        self.user_token = query.get_user_token()
        self.user_id = query.get_optional_int64('user_id', self.user_token.user_id)
        self.limit = query.get_optional_int64('limit', 100)
        self.cursor = query.get_optional_str('cursor', u'')


class GetExternalFriendsSession(POSTSession):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)
        self._params = Params()

    def _init_session_params(self, query):
        self._params.parse(query)

    def _run_session(self):
        info = self._users_engine.get_info(self._params.user_id)
        if info == None:
            raise APILogicalError('User {} not found'.format(self._params.user_id))

        friends = list()
        cursor = ''
        has_next = False

        if info.facebook_user_id != None and info.facebook_access_token != None:
            friends, cursor, has_next = self._get_fb_friends(info.facebook_user_id, info.facebook_access_token)

        result = {
            'success': True,
            'data': friends,
            'paging': { 'cursor': cursor, 'has_next': has_next }
        }
        return result

    def _get_fb_friends(self, facebook_user_id, facebook_access_token):
        friends = list()

        page_uri = 'https://graph.facebook.com/{}/friends?access_token={}&limit={}&after={}'.format(facebook_user_id, facebook_access_token, self._params.limit, self._params.cursor)
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

            local_ids = self._users_engine.external_to_local_id_many(external_ids)
            for i in range(len(local_ids) - 1, -1, -1):
                if local_ids[i] == None:
                    del local_ids[i]
                    del profiles[i]
                profiles[i]['user_id'] = local_ids[i]

            check_result = self._users_engine.check_friends_many(self._params.user_id, local_ids)
            for i in range(len(check_result)):
                if not check_result[i]:
                    friends.append(profiles[i])

            # paging
            cursor, has_next = self._parse_paging(parsed.get('paging'))
        except Exception as ex:
            logging.error(ex)
            raise

        return (friends, cursor, has_next)

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
    s.parse_query('{"user_token": "4msU7R3qOnQ6o+QDS76PUtgcrnbVSh9lAkxM4peLfJQhLn6XiIWp3ZVN71W/YGmytfP7KT/zNUb3fXydyvdtn7lNKLYTXuj8Eoa6mifuNrCbg8ItRllEhImy/cSFWUCa"}')
    print s.execute()

