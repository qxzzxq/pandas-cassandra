import unittest

import pandra.cassandra_connector


class TestCassandraConnector(unittest.TestCase):

    def test_default_data_type(self):
        my_int_type = pandra.cassandra_connector.IntegerType('myIntegerType', primary_key=True)
        self.assertTrue(my_int_type.primary_key)
        self.assertEqual(my_int_type.column_type, 'int')
        self.assertEqual(my_int_type.name, 'myIntegerType')

    def test_customized_data_type(self):
        class MyDataType(pandra.cassandra_connector.DataType):
            def __init__(self, name, primary_key=False):
                super(MyDataType, self).__init__(name, 'cassandra_data_type', primary_key)

        mydatatype = MyDataType('mydatatype')
        self.assertFalse(mydatatype.primary_key)
        self.assertEqual(mydatatype.column_type, 'cassandra_data_type')
        self.assertEqual(mydatatype.name, 'mydatatype')

    def test_Cassandra_Table(self):
        class MyDataType(pandra.cassandra_connector.DataType):
            def __init__(self, name, primary_key=False):
                super(MyDataType, self).__init__(name, 'cassandra_data_type', primary_key)

        class User(pandra.cassandra_connector.CassandraTable):
            id = pandra.cassandra_connector.IntegerType('id', primary_key=True)
            name = pandra.cassandra_connector.StringType('username', primary_key=True)
            email = pandra.cassandra_connector.StringType('email')
            password = pandra.cassandra_connector.StringType('password')
            mydatatype = MyDataType('mydatatype')

        cql_create = User.create(key_space="myKeySpace")
        expected_resp = 'CREATE TABLE myKeySpace.User ( id int, username text, ' \
                        'email text, password text, mydatatype cassandra_data_type, ' \
                        'PRIMARY KEY ( id, username ) );'

        self.assertEqual(cql_create, expected_resp)

        u = User(id=123456, name='Michael', email='test@orm.org', password='my-pwd', mydatatype=1)
        cql_insert, values = u.insert()
        expected_insert = 'INSERT INTO User (id, username, email, password, ' \
                          'mydatatype) VALUES (%s, %s, %s, %s, %s)'
        expected_values = [123456, 'Michael', 'test@orm.org', 'my-pwd', 1]
        self.assertEqual(cql_insert, expected_insert)
        self.assertEqual(values, expected_values)


if __name__ == "__main__":
    unittest.main()
