# Python Pandas-Cassandra connector

## Usage

### Convert pandas DF to cassandra
```python
import pandra as cql
from cassandra.cluster import Cluster

cluster = Cluster([
                   'ip_cluster'
                   ])

cassandra_session = cluster.connect('name_space')

test = [
    {'a': 1, 'b': 2, 'c': 'dfs'},
    {'a': 4, 'b': 7, 'c': 'test'},
]

cql_df = cql.DataFrame.from_dict(test)

column_types = {
    'a': cql.IntegerType('a', primary_key=True),
    'b': cql.IntegerType('b'),
    'c': cql.StringType('c')
}

cql_df.to_cassandra(cassandra_session=cassandra_session,
                    table_name='test_table',
                    data_types=column_types,
                    debug=False,
                    create_table=True)
```

### Define your proper DataType and Model
```python
import pandra as cql


# Define your own class
class User(cql.Model):
    id = cql.IntegerType('id', primary_key=True)
    name = cql.StringType('username', primary_key=True)
    email = cql.StringType('email')
    password = cql.StringType('password')
    

# Create a new User object
u = User(id=123456, name='Michael', email='test@orm.org', password='my-pwd', test_attr = 1)

# Create table User
u.create()

# Insert data to table User
u.insert()
```
