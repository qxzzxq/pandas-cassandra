# Python Pandas-Cassandra connector

## Usage
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

