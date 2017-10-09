#!/usr/bin/env python
# encoding: utf-8

from tornado.httpclient import AsyncHTTPClient
from tornado import gen
import json, base64

instance = None

class BitcoinRPC(object):

    def __init__(self, host, port, user, password, max_client=10, http_client='simple'):
        if max_client:
            AsyncHTTPClient.configure(None, max_clients=max_client)
        if http_client == 'curl':
            AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")
        else:
            pass
        self.bitcoin_url = 'http://%s:%d' % (host, port)
        self.user = user
        self.password = password
        self.headers = {
                'Content-Type': 'text/json', 
                'Authorization': 'Basic %s' % base64.b64encode("%s:%s" % (self.user, self.password))
            }
    
    @gen.coroutine
    def _call_raw(self, data):
        r = yield AsyncHTTPClient().fetch(self.bitcoin_url, method='POST', body=data, headers=self.headers)
        if r.code== 200:
            raise gen.Return(json.loads(r.body)['result'])

        else:
            raise gen.Return(False)

    @gen.coroutine
    def _call(self, method, params):
        result = yield self._call_raw(json.dumps({
                'jsonrpc':  '2.0',
                'method':   method,
                'params':   params,
                'id':       '1'
            }))
        raise gen.Return(result)
    
    @gen.coroutine
    def latest_2_block(self):
        block_count = yield self.getblockcount()
        blocks = []
        for block_height in (block_count - 1, block_count):
            blocks.append({'hash': (yield self.getblockhash(block_height)).decode('hex')[::-1], 'height': block_height})
        raise gen.Return(blocks)
    
    @gen.coroutine
    def latest_block(self):
        block_count = yield self.getblockcount()
        block_hash = yield self.getblockhash(block_count)
        raise gen.Return({'hash': block_hash.decode('hex')[::-1], 'height': block_count})

    @gen.coroutine
    def getblockcount(self):
        data = yield self._call('getblockcount', [])
        raise gen.Return(data)

    @gen.coroutine
    def getblockhash(self, block_height):
        block = yield self._call('getblockhash', [block_height])
        raise gen.Return(block)

    @gen.coroutine
    def getblock(self, block_hash, israw=False):
        rawblock = yield self._call('getblock', [block_hash, israw])
        raise gen.Return(rawblock)

    @gen.coroutine
    def getrawtransaction(self, tx_id):
        tx = yield self._call('getrawtransaction', [tx_id])
        raise gen.Return(tx)
