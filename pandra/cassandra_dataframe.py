# coding: utf-8

import logging

import cassandra
import pandas as pd

from . import cassandra_connector as cql_connector

logger = logging.getLogger(__name__)


class CassandraDataFrame(pd.DataFrame):
    """
    CassandraDataFrame is a subclass of pandas.DataFrame.
    It includes methods to interface with Cassandra database
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
        connector = type(table_name, (cql_connector.CassandraTable,), data_types)
        if create_table:
            cql_create = connector.create()
            if not debug:
                try:
                    cassandra_session.execute(cql_create)
                    logger.info('Create new table {}'.format(table_name))
                except cassandra.AlreadyExists:
                    logger.warning('Table {} already exists.'.format(table_name))

        for index, row in self.iterrows():
            row_to_insert = connector(**row.to_dict())
            cql_insert, values = row_to_insert.insert()
            if not debug:
                try:
                    cassandra_session.execute(cql_insert, values)
                except Exception as e:
                    logger.warning(e)

        logger.info('Successfully inserted {} rows.'.format(self.shape[0]))

    @classmethod
    def from_cassandra(cls, cassandra_session, cql_command, async=False):
        # TODO: add async method
        # TODO: auto detect column type
        raw_query = cassandra_session.execute(cql_command)
        frame = list(raw_query)[:]
        return cls(frame)
