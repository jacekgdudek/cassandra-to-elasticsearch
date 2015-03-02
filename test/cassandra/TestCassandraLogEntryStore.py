import random
import uuid
from dateutil import parser
from time_uuid import TimeUUID
from hamcrest import *

from app.cassandra.CassandraLogEntry import CassandraLogEntry


class TestCassandraLogEntryStore(object):

    def test_create_log_entry(self, cassandra_log_entry_store):
        entry = self.build_log_entry(time_uuid=uuid.uuid1(),
                                     logged_keyspace="test_keyspace",
                                     logged_table="test_table",
                                     logged_key=str(uuid.uuid4()),
                                     operation="save",
                                     updated_columns={"a", "b", "c"})
        cassandra_log_entry_store.create(entry)

        rows = cassandra_log_entry_store.find_by_logged_row(entry.logged_keyspace, entry.logged_table, entry.logged_key)
        assert len(rows) == 1
        assert rows[0] == entry

    def test_find_log_entries_filtering_by_minimum_time(self, cassandra_log_entry_store):
        entries = list()
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-01-01T00:00:00.000000-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="save",
                                            updated_columns={"a", "b", "c"}))
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-01-02T00:00:00.000000-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="save",
                                            updated_columns={"a", "b", "c"}))
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-01-02T15:59:59.999999-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="save",
                                            updated_columns={"a", "b", "c"}))
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-01-02T16:00:00.000000-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="save",
                                            updated_columns={"a", "b", "c"}))
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-01-03T00:00:00.000000-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="save",
                                            updated_columns={"a", "b", "c"}))
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-01-04T00:00:00.000000-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="save",
                                            updated_columns={"a", "b", "c"}))
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-01-05T00:00:00.000000-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="save",
                                            updated_columns={"a", "b", "c"}))
        entries.append(self.build_log_entry(time_uuid=self._create_time_uuid("2015-01-05T15:00:00.000000-0000"),
                                            logged_keyspace="test_keyspace",
                                            logged_table="test_table",
                                            logged_key=str(uuid.uuid4()),
                                            operation="save",
                                            updated_columns={"a", "b", "c"}))

        shuffled_entries = list(entries)
        random.shuffle(shuffled_entries)
        for entry in shuffled_entries:
            cassandra_log_entry_store.create(entry)

        time = parser.parse("2015-01-02T16:00:00.000000-0000")
        found_entries = cassandra_log_entry_store.find_by_time_greater_or_equal_than(time)

        assert_that(found_entries, has_length(greater_than_or_equal_to(5)))
        assert_that(found_entries, not(has_items(entries[0], entries[1], entries[2])))
        assert_that(found_entries, has_items(entries[3], entries[4], entries[5], entries[6], entries[7]))

    @staticmethod
    def _create_time_uuid(date_time_string):
        return TimeUUID.convert(parser.parse(date_time_string))

    @staticmethod
    def build_log_entry(time_uuid, logged_keyspace, logged_table, logged_key, operation, updated_columns):
        log_entry = CassandraLogEntry()
        log_entry.time_uuid = time_uuid
        log_entry.logged_keyspace = logged_keyspace
        log_entry.logged_table = logged_table
        log_entry.logged_key = logged_key
        log_entry.operation = operation
        log_entry.updated_columns = updated_columns
        return log_entry