import logging
import yaml

DEFAULT_ENVIRONMENT = "development"
DEFAULT_CASSANDRA_LOG_KEYSPACE = "logger"
DEFAULT_CASSANDRA_LOG_TABLE = "log"
DEFAULT_INTERVAL_BETWEEN_RUNS = 10
DEFAULT_CASSANDRA_ID_COLUMN_NAME = "id"
DEFAULT_CASSANDRA_TIMESTAMP_COLUMN_NAME = "timestamp"
DEFAULT_SEND_GRID_API_KEY = ""
DEFAULT_SEND_GRID_EMAIL_TO = ""
DEFAULT_SEND_GRID_EMAIL_FROM = "Cassandra_To_Elasticsearch_Sync"
DEFAULT_SEND_GRID_EMAIL_TITLE = "Sync Failure"
DEFAULT_SLACK_API_KEY = ""
DEFAULT_SLACK_BOT_NAME = "bot"
DEFAULT_SLACK_CHANNEL = "#bots"


class Settings(object):

    def __init__(self, dictionary):

        self._environment = dictionary.get("environment", DEFAULT_ENVIRONMENT)

        self._interval_between_runs = dictionary.get("interval_between_runs", DEFAULT_INTERVAL_BETWEEN_RUNS)
        self._cassandra_log_keyspace = dictionary.get("cassandra_log_keyspace", DEFAULT_CASSANDRA_LOG_KEYSPACE)
        self._cassandra_log_table = dictionary.get("cassandra_log_table", DEFAULT_CASSANDRA_LOG_TABLE)
        self._cassandra_id_column_name = dictionary.get("cassandra_id_column_name", DEFAULT_CASSANDRA_ID_COLUMN_NAME)

        self._sendgrid_api_key = dictionary.get("sendgrid_api_key", DEFAULT_SEND_GRID_API_KEY)
        self._sendgrid_email_to = dictionary.get("sendgrid_email_to", DEFAULT_SEND_GRID_EMAIL_TO)
        self._sendgrid_email_from = dictionary.get("sendgrid_email_from", DEFAULT_SEND_GRID_EMAIL_FROM)
        self._sendgrid_email_title = dictionary.get("sendgrid_email_title", DEFAULT_SEND_GRID_EMAIL_TITLE)

        self._slack_api_key = dictionary.get("slack_api_key", DEFAULT_SLACK_API_KEY)
        self._slack_bot_name = dictionary.get("slack_bot_name", DEFAULT_SLACK_BOT_NAME)
        self._slack_channel = dictionary.get("slack_channel", DEFAULT_SLACK_CHANNEL)

        self._cassandra_timestamp_column_name = \
            dictionary.get("cassandra_timestamp_column_name", DEFAULT_CASSANDRA_TIMESTAMP_COLUMN_NAME)

    @property
    def environment(self):
        return self._environment

    @property
    def interval_between_runs(self):
        return self._interval_between_runs

    @property
    def cassandra_log_keyspace(self):
        return self._cassandra_log_keyspace

    @property
    def cassandra_log_table(self):
        return self._cassandra_log_table

    @property
    def cassandra_id_column_name(self):
        return self._cassandra_id_column_name

    @property
    def cassandra_timestamp_column_name(self):
        return self._cassandra_timestamp_column_name

    @property
    def sendgrid_api_key(self):
        return self._sendgrid_api_key
    @property
    def sendgrid_email_to(self):
        return self._sendgrid_email_to
    @property
    def sendgrid_email_from(self):
        return self._sendgrid_email_from
    @property
    def sendgrid_email_title(self):
        return self._sendgrid_email_title


    @property
    def slack_api_key(self):
        return self._slack_api_key
    @property
    def slack_bot_name(self):
        return self._slack_bot_name
    @property
    def slack_channel(self):
        return self._slack_channel
    

    @classmethod
    def load_from_file(cls, filename):
        logger = logging.getLogger()
        logger.info("Loading settings from file %s", filename)
        stream = open(filename, 'r')
        return Settings(yaml.load(stream))
