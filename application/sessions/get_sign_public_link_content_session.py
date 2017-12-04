import json
import re
import jinja2
import logging


class GetSignPublicLinkContentQuery(object):

    def __init__(self):
        self.sign_id = None

    def parse(self, sign_id):
        self.sign_id = sign_id


class GetSignPublicLinkContentSession(object):

    def __init__(self, global_context):
        pass

    def parse_query(self, sign_id):
        self._query = GetSignPublicLinkContentQuery()
        self._query.parse(sign_id)

    def execute(self):
        # TODO check sign exists

        env = jinja2.Environment(
            loader=jinja2.FileSystemLoader('templates/')
        )

        context = {
            'PREVIEW_IMAGE_URL': 'http://api.quest.aiarlabs.com/content/sign/{}/preview'.format(self._query.sign_id),
            'APPLINK_URL': 'signquest://pathtosign/id/{}'.format(self._query.sign_id),
        }

        template = env.get_template('100502.html')
        return template.render(context)


if __name__ == "__main__":
    from global_context import GlobalContext
    global_context = GlobalContext()
    global_context.initialize()

    s = GetSignPublicLinkContentSession(global_context)
    s.parse_query(1234)
    print s.execute()

