# coding: utf-8

import pandas as pd


class DataFrame(pd.DataFrame):
    """
    Add a new methods to pandas DataFrame object.
    """

    def _cqlsh_create_table(self, key_space, table_name, data_types, primary_key):
        """
        Generate CREATE TABLE commande to execute

        :param key_space:
        :param table_name:
        :param data_types: dict
        :param primary_key: name of the primary key
        :return:
        """

        if not isinstance(data_types, dict):
            raise TypeError('data_type should be a {"column_name": "type", ...} like dict')

        key_table = '{}.{}'.format(key_space, table_name) if key_space else table_name

        col_names_and_type = []
        for column_name, dtype in data_types.items():
            if column_name == primary_key:
                col_names_and_type.append('{} {} PRIMARY KEY'.format(column_name, dtype))
            else:
                col_names_and_type.append('{} {}'.format(column_name, dtype))

        command_string = \
            'CREATE TABLE {table} ( {column_name_and_type} );'.format(table=key_table,
                                                                      column_name_and_type=', '.join(col_names_and_type))
        return command_string

    def to_cassandra(self, cassandra_session):

        columns = self.columns

        for index, row in self.iterrows():
            pass

        pass

    @classmethod
    def from_s3(cls, data):
        pass
