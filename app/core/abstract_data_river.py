from abc import ABCMeta
from time import time


class AbstractDataRiver(object):

    __metaclass__ = ABCMeta

    def __init__(self, update_fetcher, update_applier):
        self._update_fetcher = update_fetcher
        self._update_applier = update_applier

    def propagate_updates(self, minimum_timestamp=None, batch_upload=True):
        last_update_timestamp = time()

        updates = self._update_fetcher.fetch_updates(minimum_timestamp)

        if batch_upload is True:
            self._update_applier.batch_apply_update(updates)
        else:
            for update in updates:
                self._update_applier.apply_update(update)
                if update.timestamp > last_update_timestamp:
                    last_update_timestamp = update.timestamp

        return last_update_timestamp
