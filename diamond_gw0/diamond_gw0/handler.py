#!/usr/bin/env python
# coding=utf-8

"""
Send metrics to a groundw0rk endpoint via POST
"""
import requests
from diamond.handler.Handler import Handler

class Gw0PostHandler(Handler):

    # Inititalize Handler with url and batch size
    def __init__(self, config=None):
        Handler.__init__(self, config)
        self.metrics = []
        self.batch_size = int(self.config['batch'])
        self.url = self.config.get('url')
        self.meta = {}
        for k, v in self.config.items():
            if k.startswith('meta_'):
                self.meta[k.replace('meta_', '')] = v

    def get_default_config_help(self):
        """
        Returns the help text for the configuration options for this handler
        """
        config = super(Gw0PostHandler, self).get_default_config_help()

        config.update({
            'url': 'Fully qualified url to send metrics to',
            'batch': 'How many to store before sending to the graphite server',
        })

        return config

    def get_default_config(self):
        """
        Return the default config for the handler
        """

        config = super(Gw0PostHandler, self).get_default_config()

        config.update({
            'url': 'http://localhost/blah/blah/blah',
            'batch': 100,
        })

        return config

    # Join batched metrics and push to url mentioned in config
    def process(self, metric):
        self.log.debug('-> %s', metric)
        self.metrics.append(str(metric))
        if len(self.metrics) >= self.batch_size:
            self.post()

    # Overriding flush to post metrics for every collector.
    def flush(self):
        """Flush metrics in queue"""
        self.post()

    def post(self):
        data = []
        for d in self.metrics:
            metric, val, ts = d.strip().split(' ')
            data.append({
                'name': metric,
                'value': val,
                'ts': ts,
                'meta': self.meta
            })
        requests.post(self.url, json=data)
        self.metrics = []
