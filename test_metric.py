import unittest
import os
os.environ['DATA_DIR'] = 'db-test'
os.environ['DB_DIR'] = 'db-test'
from utils import b

print(b([os.environ['DB_DIR'], b'index.db']))

INDEX_DB = b'/'.join(b([os.environ['DB_DIR'], b'index.db']))

if os.path.exists(INDEX_DB):
    os.remove(INDEX_DB)

from worker import db_conn, prepare_db, update_db_index, get_metric_props_as_dict, find_metrics


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
        assert mp['host'] == 'foobarhost'
        assert mp['test'] == 1
        assert mp['hostid'] == 123
        assert mp['floatval'] == 1.299

        metrics = find_metrics('customer_12', 'foo.*.baz')
        assert metrics == ['foo.bar.baz']

        metrics = find_metrics('customer_12', 'foo.*')
        assert metrics == ['foo.bar.baz']

        metrics = find_metrics('customer_12', None, [
            ('host', '=', 'foobarhost')
        ])
        assert metrics == ['foo.bar.baz']

        metrics = find_metrics('customer_12', None, [
            ('hostid', '>', 121)
        ])
        assert metrics == ['foo.bar.baz']

        metrics = find_metrics('customer_12', None, [
            ('floatval', '>', 1.2)
        ])
        assert metrics == ['foo.bar.baz']

        metrics = find_metrics('customer_12', None, [
            ('floatval', '>', 1.2),
            ('hostid', '>', 121)
        ])
        assert metrics == ['foo.bar.baz']

        # this 'and' s metrics
        metrics = find_metrics('customer_12', None, [
            ('floatval', '>', 1.2),
            ('hostid', '>', 131)
        ])
        assert metrics == []

if __name__ == '__main__':
    unittest.main()