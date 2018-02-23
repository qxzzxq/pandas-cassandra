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
    # _metadata = ['cassandra_session', 'data_types', 'table_name']

    @property
    def _constructor(self):
        return CassandraDataFrame

    def _infer_data_type_from_dtype(self, primary_key):
        """
        Create a {"column_name": "type", ...} like dict for each column of the data
        :return:
        """
        if not primary_key:
            logger.error('No primary key is given.')
            raise ValueError('No primary key is given.')

        __dtype_mapping = {
            'bool8': 'boolean',
            'bool_': 'boolean',
            'byte': 'int',
            'bytes0': 'int',
            'bytes_': 'int',
            'cdouble': 'text',
            'cfloat': 'text',
            'clongdouble': 'text',
            'clongfloat': 'text',
            'complex128': 'text',
            'complex64': 'text',
            'complex_': 'text',
            'csingle': 'text',
            'datetime64': 'text',
            'double': 'float',
            'float16': 'float',
            'float32': 'float',
            'float64': 'float',
            'float_': 'float',
            'half': 'float',
            'int0': 'int',
            'int16': 'int',
            'int32': 'int',
            'int64': 'int',
            'int8': 'int',
            'int_': 'int',
            'intc': 'int',
            'intp': 'int',
            'longcomplex': 'text',
            'longdouble': 'double',
            'longfloat': 'double',
            'longlong': 'int',
            'matrix': 'int',
            'object': 'text',
            'record': 'text',
            'short': 'int',
            'single': 'float',
            'singlecomplex': 'text',
            'str0': 'text',
            'str_': 'text',
            'string_': 'text',
            'timedelta64': 'text',
            'ubyte': 'int',
            'uint': 'int',
            'uint0': 'int',
            'uint16': 'int',
            'uint32': 'int',
            'uint64': 'int',
            'uint8': 'int',
            'uintc': 'int',
            'uintp': 'int',
            'ulonglong': 'int',
            'unicode_': 'text',
            'ushort': 'int',
            'void': 'text',
            'void0': 'text'
        }

        column_names = self.columns.tolist()
        column_dtype = [i.name for i in self.dtypes.tolist()]
        cql_data_type = [__dtype_mapping[i] for i in column_dtype]
        __iterator = dict(zip(column_names, cql_data_type))

        data_types = {}
        for k, v in __iterator.items():
            type_of_k = cql_connector.DataType(name=k, column_type=v, primary_key=k in primary_key)
            data_types[k] = type_of_k

        return data_types

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
                     primary_key=None,
                     create_table=False,
                     debug=False):
        """
        :param cassandra_session:
        :param table_name: name of table
        :param data_types: dict of DataType object
        :type data_types: dict
        :param create_table: True to create a new table
        :param debug: True to print command, false to execute
        :param primary_key:
        :return:
        """
        if not data_types:
            data_types = self._infer_data_type_from_dtype(primary_key=primary_key)

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
