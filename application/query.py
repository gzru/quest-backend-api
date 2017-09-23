import json
import base64
import numpy


class BadQuery(Exception):
    pass


class Query:

    def _parse_json(self, data):
        try:
            return json.loads(data)
        except Exception as ex:
            raise BadQuery('Can\'t parse json')

    def _get_optional(self, tree, name):
        return tree.get(name)

    def _get_required(self, tree, name):
        value = self._get_optional(tree, name)
        if not value:
            raise BadQuery('Missed required query parameter "{}"'.format(name))
        return value

    def _check_int64(self, name, value):
        if not value:
            return None
        if isinstance(value, int) and numpy.int64(value) == value:
            return value
        raise BadQuery('"{}" have bad format'.format(name))

    def _check_float(self, name, value):
        if not value:
            return None
        if (isinstance(value, float) or isinstance(value, int)) and \
            numpy.float64(value) == value:
            return value
        raise BadQuery('"{}" have bad format'.format(name))

    def _check_str(self, name, value):
        if not value:
            return None
        if isinstance(value, unicode):
            return value
        raise BadQuery('"{}" have bad format'.format(name))

    def _get_optional_int64(self, tree, name):
        return self._check_int64(name, self._get_optional(tree, name))

    def _get_optional_float64(self, tree, name):
        return self._check_float(name, self._get_optional(tree, name))

    def _get_optional_str(self, tree, name):
        return self._check_str(name, self._get_optional(tree, name))

    def _get_optional_blob(self, tree, name):
        blob = tree.get(name)
        if not blob:
            return None
        try:
            return base64.b64decode(blob)
        except Exception as ex:
            # log error
            raise BadQuery('"{}" have bad format'.format(name))

    def _get_required_blob(self, tree, name):
        blob = self._get_required(tree, name)
        try:
            return base64.b64decode(blob)
        except Exception as ex:
            # log error
            raise BadQuery('"{}" have bad format'.format(name))

    def _get_required_int64(self, tree, name):
        return self._check_int64(name, self._get_required(tree, name))

    def _get_required_str(self, tree, name):
        return self._check_str(name, self._get_required(tree, name))

