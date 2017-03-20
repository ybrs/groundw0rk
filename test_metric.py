import unittest
import os
os.environ['DATA_DIR'] = 'db-test'
os.environ['DB_DIR'] = 'db-test'
from utils import b

print(b([os.environ['DB_DIR'], b'index.db']))

INDEX_DB = b'/'.join(b([os.environ['DB_DIR'], b'index.db']))

if os.path.exists(INDEX_DB):
    os.remove(INDEX_DB)

from worker import db_conn, prepare_db, update_db_index, get_metric_props_as_dict


class TestMetricDatabase(unittest.TestCase):

    def setUp(self):
        prepare_db()

    def test_create_select_metric(self):
        import time
        update_db_index('customer_12', 'foo.bar.baz', time.time(), 8.2, '', metric_props={
            'host': 'foobarhost',
            'test': True,
            'hostid': 123,
            'floatval': 1.299
        })

        mp = get_metric_props_as_dict('customer_12', 'foo.bar.baz')
        print(mp)


if __name__ == '__main__':
    unittest.main()