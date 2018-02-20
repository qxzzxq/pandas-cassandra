# coding: utf-8


class DataType:

    def __init__(self, name, column_type, primary_key):
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
        self.primary_key = self.__get_primary_key()

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError('{name} object has no attribute {key}'.format(name=self.__table__,
                                                                               key=key))

    def __setattr__(self, key, value):
        self[key] = value

    def __get_primary_key(self):
        key = [k for k, v in self.__mappings__.items() if v.primary_key]

        if len(key) == 0:
            raise AttributeError('{} object has no primary key'.format(self.__table__))

        # if len(key) > 1:
        #     raise AttributeError('{} object has more than one primary key'.format(self.__table__))
        return key

    def insert(self):
        fields = []
        params = []
        args = []
        for k, v in self.__mappings__.items():
            fields.append(v.name)
            params.append('%s')
            args.append(getattr(self, k, None))
        sql = 'INSERT INTO %s (%s) VALUES (%s)' % (self.__table__, ', '.join(fields), ', '.join(params))
        # print('SQL: %s' % sql)
        # print('ARGS: %s' % str(args))

        return sql, args

    def create(self, key_space=None):
        table_name = self.__table__
        key_table = '{}.{}'.format(key_space, table_name) if key_space else table_name

        col_names_and_type = [str(v) for k, v in self.__mappings__.items()]

        command_string = \
            'CREATE TABLE {table} ( {columns}, PRIMARY KEY ( {keys} ) );'.format(table=key_table,
                                                                                 columns=', '.join(col_names_and_type),
                                                                                 keys=', '.join(self.primary_key))
        return command_string
