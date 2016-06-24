from abc import abstractmethod
import abc

from elasticsearch import TransportError
import elasticsearch

from app.util.timestamp_util import TimestampUtil
from app.elasticsearch_domain.store.elasticsearch_response_util import ElasticsearchResponseUtil


MATCH_ALL_QUERY = {"query": {"match_all": {}}}


class AbstractElasticsearchStore(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, elasticsearch_client):
        self._client = elasticsearch_client

    @staticmethod
    def __build_query(op_type, index, _type, _id, body, timestamp):
        return {
            '_op_type': op_type,
            '_index': index,
            '_type': _type,
            '_id': _id,
            'doc': body,
            'timestamp': timestamp
        }

    def _base_read(self, index, _type, _id):
        try:
            response = self._client.get(index=index, doc_type=_type, id=_id, _source=True, fields="_timestamp")
            return self._process_response(response)
        except TransportError as e:
            if e.status_code == 404:
                return None
            else:
                raise

    def _base_create(self, index, _type, _id, document, prepare_query=False):
        timestamp = self.__get_timestamp(document)
        body = self._to_request_body(document)
        if prepare_query is True:
            return self.__build_query(
                'create',
                index=index,
                _type=_type,
                _id=_id,
                body=body,
                timestamp=timestamp)
        else:
            self._client.create(index=index, doc_type=_type, id=_id, body=body, timestamp=timestamp, refresh=True)

    def _base_update(self, index, _type, _id, document, prepare_query=False):
        timestamp = self.__get_timestamp(document)
        body = self._to_request_body(document)
        if prepare_query is True:
            return self.__build_query(
                'update',
                index=index,
                _type=_type,
                _id=_id,
                body=body,
                timestamp=timestamp)
        
        else:
            self._client.update(index=index, doc_type=_type, id=_id, body={"doc": body}, timestamp=timestamp, refresh=True)

    def _base_upsert(self, index, _type, _id, document, prepare_query=False):
        timestamp = self.__get_timestamp(document)
        body = self._to_request_body(document)
        if prepare_query is True:
            return self.__build_query(
                'index',
                index=index,
                _type=_type,
                _id=_id,
                body=body,
                timestamp=timestamp
            )
        
        else:
            self._client.update(index=index, doc_type=_type, id=_id, body={"doc": body}, timestamp=timestamp, refresh=True)

    def _base_delete(self, index, _type, _id, prepare_query=False):
        if prepare_query is True:
            return self.__build_query(
                'delete',
                index=index,
                _type=_type,
                _id=_id,
                body=body,
                timestamp=timestamp)
        else:
            return self._client.delete(index=index, doc_type=_type, id=_id, refresh=True)

    def _base_batch(self, actions):
        elasticsearch.helpers.bulk(
            self._client,
            actions
        )
        print "batch done"

    def _base_delete_all(self, index, _type):
        self._client.delete_by_query(index=index, doc_type=_type, body=MATCH_ALL_QUERY)

    @abstractmethod
    def _to_request_body(self, document):
        pass

    @abstractmethod
    def _from_response(self, identifier, timestamp, source):
        pass

    def _process_response(self, response):
        if not response["found"]:
            return None

        identifier = ElasticsearchResponseUtil.extract_identifier(response)
        timestamp = ElasticsearchResponseUtil.extract_timestamp(response)
        source = ElasticsearchResponseUtil.extract_source(response)
        return self._from_response(identifier, timestamp, source)

    @staticmethod
    def __get_timestamp(document):
        if document.timestamp:
            return TimestampUtil.seconds_to_milliseconds(document.timestamp)
        else:
            return None
