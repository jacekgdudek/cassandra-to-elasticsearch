from abc import ABCMeta, abstractmethod
import logging
from app.core.model.field import Field

from time import time


class AbstractUpdateApplier(object):

    __metaclass__ = ABCMeta

    def __init__(self, document_store):
        self._document_store = document_store
        self._logger = logging.getLogger()

    def apply_update(self, update, prepare_query=False):
        self._check_namespace_and_table_exists(update.identifier)
        existing_document = self._document_store.read(update.identifier)
        if existing_document:
            return self.__apply_update_to_existing_document(update, existing_document, prepare_query)
        else:
            return self.__apply_update_to_nonexistent_document(update, prepare_query)

    def apply_upsert_or_delete(self, update):
        """
        Return a query for bulk action that either upserts or deletes based on the log
        """
        if update.is_delete:
            result = self._document_store.delete(update.identifier, prepare_query=True)
        else:
            result = self._document_store.upsert(update, prepare_query=True)

        return result


    def batch_apply_update(self, updates):
        """
        Applies changes to the store using a batch approach
        """
        MAX_ITEM_IN_BATCH_UPDATE = 2000

        start_time = time()
        queries = []
        for update in updates:
            loop_start = time()

            query = self.apply_upsert_or_delete(
                update
            )

            # query might be None if it doesn't need updating
            if query:
                queries.append(query)

            loop_end = time()
            print "Prep time of one entry took {}s".format(
                loop_end-loop_start
            )
            # run batch of updates
            # TODO:// update timestamp of the last updated version now in case something goes wrong later!!!
            if len(queries) > MAX_ITEM_IN_BATCH_UPDATE:
                batch_time = time()
                print "Preparing batch took {}s".format(
                    batch_time-start_time
                )
                batch_start = time()
                self._document_store.batch(queries)
                batch_end = time()
                print "Sending batch took {}s".format(
                    batch_end - batch_start
                )
                queries = []

        self._document_store.batch(queries)

    def __apply_update_to_existing_document(self, update, existing_document, prepare_query):
        if update.timestamp > existing_document.timestamp:
            if update.is_delete:
                return self._document_store.delete(update.identifier, prepare_query)
            else:
                # in order to avoid deadlocks and break cycles, a document is updated only if there are differences.
                if not Field.fields_are_identical(existing_document.fields, update.fields):
                    return self._document_store.update(update, prepare_query)

    def __apply_update_to_nonexistent_document(self, update, prepare_query):
        if update.is_delete:
            # Document already deleted. No action needed.
            pass
        else:
            return self._document_store.create(update, prepare_query)

    @abstractmethod
    def _check_namespace_and_table_exists(self, identifier):
        """
        Makes sure the update namespace and table exists on the target database. Raises an exception if not.
        """
        pass
