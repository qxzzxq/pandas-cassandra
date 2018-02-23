# coding: utf-8

import logging

logger = logging.getLogger(__name__)


# def get_dtype_mapping(np):
#     """Get a mapping dict for numpy.dtypes and python types"""
#     __dtypes = []
#     __python_types = []
#     for name in dir(np):
#         obj = getattr(np, name)
#         if hasattr(obj, 'dtype'):
#             try:
#                 if 'time' in name:
#                     npn = obj(0, 'D')
#                 else:
#                     npn = obj(0)
#                 nat = npn.item()
#                 __dtypes.append(name)
#                 __python_types.append(nat.__class__.__name__)
#             except:
#                 pass
#
#     return dict(zip(__dtypes, __python_types))


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
        return '{} {}'.format(self.name, self.column_type)


class TextType(DataType):
    """	UTF-8 encoded string"""
    def __init__(self, name, primary_key=False):
        super(TextType, self).__init__(name, 'text', primary_key)


class IntegerType(DataType):
    """32-bit signed integer"""
    def __init__(self, name, primary_key=False):
        super(IntegerType, self).__init__(name, 'int', primary_key)


class FloatType(DataType):
    """	32-bit IEEE-754 floating point Java type"""
    def __init__(self, name, primary_key=False):
        super(FloatType, self).__init__(name, 'float', primary_key)


class BoolType(DataType):
    """true or false"""

    def __init__(self, name, primary_key=False):
        super(BoolType, self).__init__(name, 'boolean', primary_key)


class DoubleType(DataType):
    """64-bit IEEE-754 floating point Java type"""

    def __init__(self, name, primary_key=False):
        super(DoubleType, self).__init__(name, 'double', primary_key)


class ListType(DataType):
    """
    A collection of one or more ordered elements: [literal, literal, literal]

    CAUTION:
        Lists have limitations and specific performance considerations.
        Use a frozen list to decrease impact. In general, use a set instead of list.
    """

    def __init__(self, name, primary_key=False):
        super(ListType, self).__init__(name, 'list', primary_key)


class MapType(DataType):
    """	A JSON-style array of literals: { literal : literal, literal : literal ... }"""

    def __init__(self, name, primary_key=False):
        super(MapType, self).__init__(name, 'map', primary_key)


class SetType(DataType):
    """A collection of one or more elements: { literal, literal, literal }"""

    def __init__(self, name, primary_key=False):
        super(SetType, self).__init__(name, 'set', primary_key)


class UuidType(DataType):
    """A UUID in standard UUID format"""

    def __init__(self, name, primary_key=False):
        super(UuidType, self).__init__(name, 'uuid', primary_key)


class VarcharType(DataType):
    """UTF-8 encoded string"""

    def __init__(self, name, primary_key=False):
        super(VarcharType, self).__init__(name, 'varchar', primary_key)


class InetType(DataType):
    """
    IP address string in IPv4 or IPv6 format,
    used by the python-cql driver and CQL native protocols
    """

    def __init__(self, name, primary_key=False):
        super(InetType, self).__init__(name, 'inet', primary_key)


class TableFactory(type):

    def __new__(cls, name, bases, attrs):

        # Copy the attrs variable to __attrs to prevent
        # modification on the original attrs variable during
        # the creation of CassandraTable
        __attrs = dict(attrs)

        if name == 'CassandraTable':
            return type.__new__(cls, name, bases, __attrs)
        logger.info('Initiate new TableFactory : %s' % name)

        # Find all DataType in all attributes
        mappings = dict()
        primary_key = list()
        for k, v in __attrs.items():
            if isinstance(v, DataType):
                logger.debug('Found mapping: %s ==> %s' % (k, v))
                mappings[k] = v

                # Add primary keys
                if v.primary_key:
                    primary_key.append(v.name)

        for k in mappings.keys():
            __attrs.pop(k)

        __attrs['__mappings__'] = mappings
        __attrs['__table__'] = name

        # Raise error if no primary key
        if len(primary_key) == 0:
            raise AttributeError('{} object has no primary key'.format(name))

        __attrs['__primary_keys__'] = primary_key
        return type.__new__(cls, name, bases, __attrs)


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

        logger.debug("{}, {}".format(sql, args))
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
        logger.debug(command_string)
        return command_string

    def select(self):
        pass
