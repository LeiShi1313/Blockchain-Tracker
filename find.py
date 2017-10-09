# -*- coding:utf-8 -*-

from tornado import gen
from tornado.httpclient import AsyncHTTPClient, HTTPError
import tornado.ioloop
import tornado.web
from tornado.log import gen_log
from tornado.options import define, options, parse_command_line
from rpc import BitcoinRPC
from protocol import Block
import json
from dateutil.parser import parse
import time
import datetime

define('rpchost', default='127.0.0.1')
define('rpcport', default=8332)
define('rpcuser', default='user')
define('rpcpassword', default='pass')
define('max_client', default=10)
define('http_client', default='curl')
define('worker', default=1)
define('date', default='2009-1-4')



class BlockParse(object):
    def __init__(self, host, port, user, passwd, max_client, http_client):
        self.rpc = BitcoinRPC(host, port, user, passwd, max_client, http_client)
        self.pools = json.load(open('pools.json', 'r'))

    @gen.coroutine
    def parse_block_by_index(self, index):
        try:
            block, block_hash = yield self.fetch_block_by_index(index)
        except HTTPError:
            raise gen.Return()
        timestamp, coinbase_bytes, to_addr = self.parse_block(block)
        try:
            miner = self.find_miner(coinbase_bytes.decode('unicode_escape'), to_addr)
            gen_log.info("block {} mined by {}".format(index, miner))
            raise gen.Return((index, block_hash, miner, timestamp))
        except UnicodeDecodeError:
            gen_log.debug("block {} cannot be decoded!\n".format(block_hash))

    def find_miner(self, text, to_addr):
        pools = self.pools
        for tag, value in pools['coinbase_tags'].items():
            if tag in text:
                return value['name']
        for address, value in pools['payout_addresses'].items():
            if address == to_addr:
                return value['name']
        return ''

    @gen.coroutine
    def fetch_block_by_index(self, index):
        block_hash = yield self.rpc.getblockhash(index)
        rawblock = yield self.rpc.getblock(block_hash, False)
        block = Block.parse(rawblock.decode('hex_codec'), 'header')
        raise gen.Return((block[0], block_hash))

    @gen.coroutine
    def fetch_block_by_hash(self, block_hash):
        rawblock = yield self.rpc.getblock(block_hash, False)
        block = Block.parse(rawblock.decode('hex_codec'), 'header')
        raise gen.Return(block[0])

    @gen.coroutine
    def fetch_newest_block(self):
        best_block_count = yield self.rpc.getblockcount()
        block, block_hash = yield self.fetch_block_by_index(best_block_count)
        raise gen.Return((block, block_hash))

    @gen.coroutine
    def fetch_newest(self):
        best_block_count = yield self.rpc.getblockcount()
        raise gen.Return(best_block_count)

    def parse_block(self, block):
        tx_in_0 = block.txns[0].tx_in[0]
        tx_out_0 = block.txns[0].tx_out[0]
        coinbase_bytes = tx_in_0.bytes
        to_addr = tx_out_0.pk_script.to_address
        timestamp = block.timestamp
        return timestamp, coinbase_bytes, to_addr

    @gen.coroutine
    def find(self, date):
        if isinstance(date, str):
            date = parse(date)
        date_tomorrow = date + datetime.timedelta(1)

        t1 = int(time.mktime(date.timetuple()))
        t2 = int(time.mktime(date_tomorrow.timetuple()))

        zero = yield self.fetch_block_by_index(0)
        now = yield self.fetch_newest_block()
        gen_log.info(zero[0])
        gen_log.info(now[0])


if __name__ == "__main__":
    parse_command_line()
    gen_log.info("Parsing blocks...")
    blockparser = BlockParse(options.rpchost, options.rpcport,
            options.rpcuser, options.rpcpassword,
                             options.max_client, options.http_client)
    ioLoop = tornado.ioloop.IOLoop.instance()
    ioLoop.add_callback(blockparser.find, options.date)
    ioLoop.start()
