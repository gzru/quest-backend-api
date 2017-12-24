from elasticsearch import Elasticsearch 
from error import APIInternalServicesError
import logging


class SearchEntry(object):

    def __init__(self):
        self.user_id = None
        self.name = None
        self.rank = None

    def __repr__(self):
        return "(user_id = {}, rank = {})".format(self.user_id, self.rank)


class ElasticsearchConnector(object):

    def __init__(self, hosts, timeout_sec, enabled=True):
        self._enabled = enabled
        if self._enabled == False:
            return

        self._client = Elasticsearch(hosts,
                                     timeout=timeout_sec,
                                     sniff_on_start=True,
                                     sniff_on_connection_fail=True,
                                     sniffer_timeout=60)
        self._index = 'users'
        self._doc_type = 'profile'

    def set_user_name(self, user_id, name):
        if self._enabled == False:
            return

        doc = {
            'name': name
        }

        try:
            self._client.index(index=self._index, doc_type=self._doc_type, id=user_id, body=doc)
        except Exception as ex:
            logging.error('set_user_name: {}'.format(ex))
            raise APIInternalServicesError('Can\'t send data to Elasticsearch')

    def search_users_by_name(self, name):
        if self._enabled == False:
            return list()

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
            logging.error('search_users_by_name: {}'.format(ex))
            raise APIInternalServicesError('Can\'t select data from Elasticsearch')

    def _parse_es_search_response(self, response):
        hits = response.get('hits')
        if hits == None:
            raise APIInternalServicesError('Bad Elasticsearch response, no hits section')

        entries = hits.get('hits')
        if entries == None:
            raise APIInternalServicesError('Bad Elasticsearch response, no hits.hits section')

        results = list()
        for entry in entries:
            rentry = SearchEntry()
            rentry.user_id = entry['_id']
            rentry.name = entry['_source']['name']
            rentry.rank = entry['_score']
            results.append(rentry)
        return results


