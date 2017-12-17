from elasticsearch import Elasticsearch 
import logging


class SearchEntry(object):

    def __init__(self):
        self.user_id = None
        self.rank = None

    def __repr__(self):
        return "(user_id = {}, rank = {})".format(self.user_id, self.rank)


class ElasticsearchConnector(object):

    def __init__(self, hosts, timeout_sec):
        self._client = Elasticsearch(hosts,
                                     timeout=timeout_sec,
                                     sniff_on_start=True,
                                     sniff_on_connection_fail=True,
                                     sniffer_timeout=60)
        self._index = 'users'
        self._doc_type = 'profile'

    def set_user_name(self, user_id, name):
        doc = {
            'name': name
        }

        try:
            self._client.index(index=self._index, doc_type=self._doc_type, id=user_id, body=doc)
        except Exception as ex:
            # TODO
            raise

    def search_users_by_name(self, name):
        query = {
            'query': {
                'bool': {
                    'should': [ 
                        {
                            'match': {
                                "name": {'query': name, 'boost': 3}
                            }
                        },
                        {
                            'match': {
                                "name.partial": name
                            }
                        }
                    ]
                }
            }
        }

        try:
            response = self._client.search(index=self._index, doc_type=self._doc_type, body=query)
            return self._parse_es_search_response(response)
        except Exception as ex:
            # TODO
            raise

    def _parse_es_search_response(self, response):
        hits = response.get('hits')
        if hits == None:
            raise Exception('')

        entries = hits.get('hits')
        if entries == None:
            raise Exception('')

        results = list()
        for entry in entries:
            rentry = SearchEntry()
            rentry.user_id = entry['_id']
            rentry.rank = entry['_score']
            results.append(rentry)
        return results


