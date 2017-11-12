from query import Query
from error import APILogicalError
from users_engine import UsersEngine
import json


class UpdateProfileQuery(Query):

    def __init__(self):
        self.user_id = None
        self.name = None
        self.username = None
        self.email = None
        self.meta_blob = None
        self.picture_blob = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.user_id = int(self._get_required_str(tree, 'user_token'))
        self.name = self._get_optional_str(tree, 'name')
        self.username = self._get_optional_str(tree, 'username')
        self.email = self._get_optional_str(tree, 'email')
        self.meta_blob = self._get_optional_blob(tree, 'meta_blob')
        self.picture_blob = self._get_optional_blob(tree, 'picture_blob')


class UpdateProfileSession(object):

    def __init__(self, global_context):
        self._users_engine = UsersEngine(global_context)

    def parse_query(self, data):
        self._query = UpdateProfileQuery()
        self._query.parse(data)

    def execute(self):
        user_id = self._query.user_id
        user_info = self._users_engine.get_info(user_id)
        if user_info == None:
            raise APILogicalError('User not found')

        info_updated = False
        if self._query.name != None: user_info.name = self._query.name; info_updated = True
        if self._query.username != None: user_info.username = self._query.username; info_updated = True
        if self._query.email != None: user_info.email = self._query.email; info_updated = True
        if info_updated:
            self._users_engine.put_info(user_info)

        if self._query.meta_blob:
            self._users_engine.put_meta(user_id, self._query.meta_blob)

        if self._query.picture_blob != None:
            self._users_engine.put_picture(user_id, self._query.picture_blob)

        return json.dumps({'success': True, 'user_token': str(user_id)})


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = UpdateProfileSession(global_context)
    s.parse_query('{"user_token": "6823619285704494896", "name": "Ivan Ivanov"}')
    print s.execute()

