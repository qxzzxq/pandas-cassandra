# coding: utf-8


def cqlsh_create_table(key_space, table_name, data_types, primary_key):
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


class DataType:

    def __init__(self, name, column_type):
        self.name = name
        self.column_type = column_type

    def __str__(self):
        return '<%s:%s>' % (self.__class__.__name__, self.name)


class StringType(DataType):

    def __init__(self, name):
        super(StringType, self).__init__(name, 'text')


class IntegerType(DataType):

    def __init__(self, name):
        super(IntegerType, self).__init__(name, 'int')


class FloatType(DataType):

    def __init__(self, name):
        super(FloatType, self).__init__(name, 'float')


class MetaData:

    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __str__(self):
        return '{}: {}'.format(self.key, self.value)




class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        print('Create new model : %s' % name)

        mappings = dict()
        for k, v in attrs.items():
            if isinstance(v, DataType):
                print('Found mapping: %s ==> %s' % (k, v))
                mappings[k] = v

        for k in mappings.keys():
            attrs.pop(k)

        attrs['__mappings__'] = mappings
        attrs['__table__'] = name
        return type.__new__(cls, name, bases, attrs)


class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

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
        sql = 'insert into %s (%s) values (%s)' % (self.__table__, ', '.join(fields), ', '.join(params))
        # print('SQL: %s' % sql)
        # print('ARGS: %s' % str(args))

        return sql, args
