from abc import ABCMeta
from time import time


class AbstractDataRiver(object):

    __metaclass__ = ABCMeta

    def __init__(self, update_fetcher, update_applier):
        self._update_fetcher = update_fetcher
        self._update_applier = update_applier

    def propagate_updates(self, minimum_timestamp=None):
        print "propagate updates after {}".format(minimum_timestamp)
        last_update_timestamp = time()

        print "{}".format(self._update_fetcher.fetch_updates(minimum_timestamp))

        for update in self._update_fetcher.fetch_updates(minimum_timestamp):
            print "aaa"
            print "update 1 {}".format(update)
            self._update_applier.apply_update(update)
            if update.timestamp > last_update_timestamp:
                last_update_timestamp = update.timestamp

        print "propagated "

        return last_update_timestamp
