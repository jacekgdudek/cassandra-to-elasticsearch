from datetime import datetime
import pytest
from app.cassandra.store.AbstractCassandraStore import AbstractCassandraStore


class ProductFixture:

    def __init__(self, _id, name, quantity, description):
        self._id = _id
        self.name = name
        self.quantity = quantity
        self.description = description
        self.created_at = None
        self.updated_at = None

    @property
    def id(self):
        return self._id

    @property
    def key(self):
        return str(self._id)


class ProductFixtureCassandraStore(AbstractCassandraStore):

    def __init__(self, nodes, keyspace, table):
        super(ProductFixtureCassandraStore, self).__init__(nodes, keyspace, table)

    def create(self, product):
        product.created_at = datetime.utcnow()
        product.updated_at = product.created_at
        statement = self.prepare_statement(
            """
            INSERT INTO %s (id, name, quantity, description, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """ % self.table)
        self.execute(statement, (product.id, product.name, product.quantity, product.description,
                                 product.created_at, product.updated_at))

    def update(self, product):
        product.updated_at = datetime.utcnow()
        statement = self.prepare_statement(
            """
            UPDATE %s
            SET name=?, quantity=?, description=?, updated_at=?
            WHERE id=?
            """ % self.table)
        self.execute(statement, (product.name, product.quantity, product.description, product.updated_at, product.id))

    def delete(self, product):
        statement = self.prepare_statement(
            """
            DELETE FROM %s
            WHERE id=?
            """ % self.table)
        self.execute(statement, [product.id])

    def delete_all(self):
        self.execute("TRUNCATE %s" % self.table)


@pytest.fixture(scope="session")
def product_fixture_table():
    return "product_fixture"


# noinspection PyShadowingNames
@pytest.fixture(scope="session")
def product_fixture_cassandra_store(cassandra_nodes, cassandra_fixture_keyspace, product_fixture_table):
    return ProductFixtureCassandraStore(cassandra_nodes, cassandra_fixture_keyspace, product_fixture_table)


# noinspection PyShadowingNames
@pytest.fixture(scope="session", autouse=True)
def create_product_fixture_cassandra_schema(cassandra_fixture_client,
                                            product_fixture_table, cassandra_log_trigger_name):
    cassandra_fixture_client.execute("DROP TABLE IF EXISTS product")
    cassandra_fixture_client.execute(
        """
        CREATE TABLE %s (
          id uuid PRIMARY KEY,
          name text,
          quantity int,
          description text,
          created_at timestamp,
          updated_at timestamp
        )
        """ % product_fixture_table)

    cassandra_fixture_client.execute("CREATE TRIGGER logger ON %s USING '%s'" %
                                     (product_fixture_table, cassandra_log_trigger_name))