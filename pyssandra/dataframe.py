# coding: utf-8

import logging

import cassandra
import pandas as pd


class DataFrame(pd.DataFrame):
    """
    Add a new methods to pandas DataFrame object.
    """

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

    def _create_insert_command(self, table_name, data_types):
        # Insert rows
        insert_command = 'INSERT INTO ' \
                         '{table} ({col_names}) ' \
                         'VALUES ({values})'.format(table=table_name,
                                                    col_names=', '.join(
                                                        data_types.keys()),
                                                    values=', '.join(
                                                        ['%s'] * len(data_types)))

        return insert_command

    def to_cassandra(self,
                     cassandra_session,
                     table_name=None,
                     data_types=None,
                     primary_key=None,
                     create_table=False,
                     debug=False):
        """"""

        # Check if table_name matches colnames of dataframe
        if set(self.columns) != set(data_types.keys()):
            raise KeyError('Column names do not match data types')

        # Create table
        if create_table:
            # table_name = create_table
            types = self._create_data_type(types=data_types)
            create_table_command = self._cqlsh_create_table(table_name=table_name,
                                                            data_types=types,
                                                            primary_key=primary_key)
            try:
                cassandra_session.execute(create_table_command)
            except cassandra.AlreadyExists:
                logging.WARN('Table {} already exists.'.format(table_name))

        for index, row in self.iterrows():
            sql = self._create_insert_command(table_name=table_name, data_types=data_types)
            values = tuple(row)

            try:
                if debug:
                    print(sql)
                    print(values)
                else:
                    cassandra_session.execute(sql, values)

            except Exception as e:
                logging.warning(e)

    @classmethod
    def from_s3(cls, data):
        pass
