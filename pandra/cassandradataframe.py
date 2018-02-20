# coding: utf-8

import logging

import cassandra
import pandas as pd

from . import cassandra_connector as cql_connector


class CassandraDataFrame(pd.DataFrame):
    """
    Add a new methods to pandas DataFrame object.
    """

    # Add cassandra session to CassandraDataFrame
    _metadata = ['cassandra_session', 'data_type', 'table_name']

    @property
    def _constructor(self):
        return CassandraDataFrame

    def _get_data_type(self):
        """
        Create a {"column_name": "type", ...} like dict for each column of the data
        :return:
        """

        column_names = self.columns
        column_dtype = self.dtypes
        pass

    def _create_data_type(self, types):
        """
         Create a {"column_name": "type", ...} like dict manually
        :param types: dict or tuple
        :return:
        """
        if isinstance(types, dict):
            return types
        else:
            column_names = self.columns
            return dict(zip(column_names, types))

    def to_cassandra(self,
                     cassandra_session,
                     table_name=None,
                     data_types=None,
                     create_table=False,
                     debug=False):
        """
        :param cassandra_session:
        :param table_name: name of table
        :param data_types: dict of DataType object
        :type data_types: dict
        :param create_table: True to create a new table
        :param debug: True to print command, false to execute
        :return:
        """

        # Check if table_name matches column names of DataFrame
        if set(self.columns) != set(data_types.keys()):
            raise KeyError('Column names do not match data types')

        # Check if all values of data_types are DataType object
        if not all([isinstance(v, cql_connector.DataType) for _, v in data_types.items()]):
            raise ValueError('All values of data_types should be DataType objects.')

        # Create a cassandra connector
        connector = type(table_name, (cql_connector.Model,), data_types)
        __create_table = create_table
        for index, row in self.iterrows():
            row_to_insert = connector(**row.to_dict())

            if __create_table:
                cql_create = row_to_insert.create()
                try:
                    cassandra_session.execute(cql_create)
                except cassandra.AlreadyExists:
                    logging.WARN('Table {} already exists.'.format(table_name))
                __create_table = False

            cql_insert, values = row_to_insert.insert()
            try:
                if debug:
                    print(cql_insert, values)
                else:
                    cassandra_session.execute(cql_insert, values)

            except Exception as e:
                logging.warning(e)

    @classmethod
    def from_cassandra(cls, cassandra_session, cql_command, async=False):
        # TODO: add async method
        # TODO: auto detect column type
        raw_query = cassandra_session.execute(cql_command)
        x = list(raw_query)[:]
        return pd.DataFrame(x)
