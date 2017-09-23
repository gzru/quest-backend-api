from profile_session_base import ProfileQueryBase, ProfileSessionBase
import json


class FindProfileQuery(ProfileQueryBase):

    def __init__(self):
        self.facebook_user_id = None

    def parse(self, data):
        tree = self._parse_json(data)
        self.facebook_user_id = self._get_optional_str(tree, 'facebook_user_id') 


class FindProfileSession(ProfileSessionBase):

    def parse_query(self, data):
        self._query = FindProfileQuery()
        self._query.parse(data)

    def execute(self):
        user_id = self._get_external_link(self._query.facebook_user_id)

        return json.dumps({ 'user_id': user_id })

