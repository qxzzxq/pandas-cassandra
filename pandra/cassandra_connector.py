# coding: utf-8

import logging


class DataType:
    """
    Define the metaclass of all data types
    """

    def __init__(self, name, column_type, primary_key):
        """
        :param name: Name of the column
        :param column_type: data type
        :param primary_key: true if primary key
        """
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key

    def __str__(self):
        # return '<%s:%s>' % (self.__class__.__name__, self.name)
        return '{} {}'.format(self.name, self.column_type)


class StringType(DataType):

    def __init__(self, name, primary_key=False):
        super(StringType, self).__init__(name, 'text', primary_key)


class IntegerType(DataType):

    def __init__(self, name, primary_key=False):
        super(IntegerType, self).__init__(name, 'int', primary_key)


class FloatType(DataType):

    def __init__(self, name, primary_key=False):
        super(FloatType, self).__init__(name, 'float', primary_key)


class TableFactory(type):

    def __new__(cls, name, bases, attrs):
        if name == 'CassandraTable':
            return type.__new__(cls, name, bases, attrs)
        logging.info('Create new model : %s' % name)

        # Find all DataType in all attributes
        mappings = dict()
        primary_key = list()
        for k, v in attrs.items():
            if isinstance(v, DataType):
                logging.info('Found mapping: %s ==> %s' % (k, v))
                mappings[k] = v

                # Add primary keys
                if v.primary_key:
                    primary_key.append(k)

        for k in mappings.keys():
            attrs.pop(k)

        attrs['__mappings__'] = mappings
        attrs['__table__'] = name

        # Raise error if no primary key
        if len(primary_key) == 0:
            raise AttributeError('{} object has no primary key'.format(name))

        attrs['__primary_keys__'] = primary_key
        return type.__new__(cls, name, bases, attrs)


class CassandraTable(dict, metaclass=TableFactory):

    def __init__(self, **kw):
        super(CassandraTable, self).__init__(**kw)
        # self.primary_key = self.__get_primary_key()

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError('{name} object has no attribute {key}'.format(name=self.__table__,
                                                                               key=key))

    def __setattr__(self, key, value):
        self[key] = value

    def insert(self):
        fields = []
        params = []
        args = []
        for k, v in self.__mappings__.items():
            fields.append(v.name)
            params.append('%s')
            args.append(getattr(self, k, None))
        sql = 'INSERT INTO %s (%s) VALUES (%s)' % (self.__table__, ', '.join(fields), ', '.join(params))

        return sql, args

    @classmethod
    def create(cls, key_space=None):
        table_name = cls.__table__
        key_table = '{}.{}'.format(key_space, table_name) if key_space else table_name

        col_names_and_type = [str(v) for k, v in cls.__mappings__.items()]

        command_string = \
            'CREATE TABLE {table} ( {columns}, PRIMARY KEY ( {keys} ) );'.format(table=key_table,
                                                                                 columns=', '.join(col_names_and_type),
                                                                                 keys=', '.join(cls.__primary_keys__))
        return command_string

    def select(self):
        pass
