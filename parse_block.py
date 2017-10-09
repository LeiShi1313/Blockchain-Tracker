# -*- coding:utf-8 -*-

from tornado import gen
from tornado.httpclient import AsyncHTTPClient, HTTPError
import tornado.ioloop
from tornado.ioloop import PeriodicCallback
import tornado.web
from handler import WebHandler
from tornado.log import gen_log 
from tornado.options import define, options, parse_command_line
from rpc import BitcoinRPC
from protocol import Block
import json
import motor

define('periodic', default=1000*30)
define('rpchost', default='127.0.0.1')
define('rpcport', default=8332)
define('rpcuser', default='user')
define('rpcpassword', default='pass')
define('max_client', default=10)
define('http_client', default='simple')
define('worker', default=1)
define('start', default=0)
define('end', default=0)
define('mongo_url', default='mongodb://localhost:27017')
define('mongo_database', default='blockchain')
define('mongo_collections', default='block')


class BlockParse(object):
    def __init__(self, host, port, user, passwd, max_client, http_client):
        self.rpc = BitcoinRPC(host, port, user, passwd, max_client, http_client)
        self.db = motor.MotorClient(options.mongo_url)[options.mongo_database][options.mongo_collections]
        self.pools = json.load(open('pools.json', 'r'))
        self.to_parse_blocks = set()
        self.parsing_blocks = set()
        self.parsed_blocks = set()

    @gen.coroutine
    def fetch_block_count(self):
        try:
            count = yield self.rpc.getblockcount()
        except HTTPError:
            count = -1
            pass
        raise gen.Return(count)

    @gen.coroutine
    def get_best_block(self):
        best_block_count = yield self.fetch_block_count()
        if best_block_count >= 0:
            if best_block_count not in self.parsing_blocks and best_block_count not in self.parsed_blocks:
                self.to_parse_blocks.add(best_block_count)
                gen_log.debug('{} blocks to parse'.format(len(self.to_parse_blocks)))
            else:
                gen_log.debug('No block to parse, block height {}'.format(best_block_count))

    @gen.coroutine
    def parse_block_by_index(self, index):
        try:
            block, block_hash = yield self.fetch_by_index(index)
            gen_log.info('Parsing block {}'.format(index))
        except HTTPError as e:
            if '599' in str(e):
                self.to_parse_blocks.add(index)
            if '500' not in str(e):
                gen_log.error(str(e))
            raise gen.Return()
        timestamp, coinbase_bytes, to_addr = self.parse_block(block)

        miner = self.find_miner(coinbase_bytes, to_addr)
        gen_log.info("block {} mined by {}".format(index, miner))
        yield self.save(index, block_hash, miner, timestamp)
        self.parsed_blocks.add(index)

    def find_miner(self, text, to_addr):
        pools = self.pools
        for address, value in pools['payout_addresses'].items():
            if address == to_addr:
                return value['name']
        for tag, value in pools['coinbase_tags'].items():
            try:
                if tag in text.decode('unicode_escape'):
                    return value['name']
            except UnicodeDecodeError:
                if tag in repr(text):
                    return value['name']
        gen_log.error("Coinbase {} can not be decoded!".format(repr(text)))
        return ''

    @gen.coroutine
    def fetch_by_index(self, index):
        block_hash = yield self.rpc.getblockhash(index)
        rawblock = yield self.rpc.getblock(block_hash, False)
        block = Block.parse(rawblock.decode('hex_codec'), 'header')
        raise gen.Return((block[0], block_hash))

    def parse_block(self, block):
        tx_in_0 = block.txns[0].tx_in[0]
        tx_out_0 = block.txns[0].tx_out[0]
        coinbase_bytes = tx_in_0.bytes
        to_addr = tx_out_0.pk_script.to_address
        timestamp = block.timestamp
        return timestamp, coinbase_bytes, to_addr

    @gen.coroutine
    def sync_from_database(self):
        saved = yield self.db.distinct('_id')
        self.parsed_blocks = set(saved)

    @gen.coroutine
    def save(self, index, block_hash, miner, timestamp):
        item = {
            '_id':       index,
            'hash':      block_hash,
            'miner':     miner,
            'timestamp': timestamp
        }
        try:
            yield self.db.insert(item)
        except Exception as e:
            # gen_log.error(str(e))
            pass

    @gen.coroutine
    def getone(self, index):
        block, block_hash = yield self.fetch_by_index(index)
        timestamp, coinbase_bytes, to_addr = self.parse_block(block)
        print "index:{}\thash:{}\ttimestamp:{}\n\tto_addr:{}\tbytes:{}\n".format(
            index, block_hash, timestamp, to_addr, coinbase_bytes
        )
    @gen.coroutine
    def test(self):
        cursor = self.db.find({'timestamp': {'$gte': 1433388515}})
        result = yield cursor.to_list(100)
        print result

    @gen.coroutine
    def parser_daemon(self):
        while True:
            try:
                index_to_parse = self.to_parse_blocks.pop()
                self.parsing_blocks.add(index_to_parse)
                yield self.parse_block_by_index(index_to_parse)
            except KeyError:
                yield gen.sleep(0.5)

    @gen.coroutine
    def run_listener(self, start, end):
        yield self.sync_from_database()
        gen_log.info("{} blocks parsed".format(len(self.parsed_blocks)))
        if end == start:
            end = yield self.fetch_block_count()
        for i in range(start, end):
            self.to_parse_blocks.add(i)
        self.to_parse_blocks -= self.parsed_blocks
        gen_log.info("{} blocks to parse".format(len(self.to_parse_blocks)))
        PeriodicCallback(self.get_best_block, options.periodic).start()

    @gen.coroutine
    def run_parser(self, worker):
        for _ in range(worker):
            self.parser_daemon()
            
            
def application_configure():
    return tornado.web.Application([
        (r"/", WebHandler),
    ])

if __name__ == "__main__":
    parse_command_line()
    gen_log.info("Block parser running...")
    blockparser = BlockParse(options.rpchost, options.rpcport,
            options.rpcuser, options.rpcpassword,
                             options.max_client, options.http_client)
    # application = application_configure()
    # application.listen(8088)
    ioLoop = tornado.ioloop.IOLoop.instance()
    ioLoop.add_callback(blockparser.run_listener, options.start, options.end)
    ioLoop.add_callback(blockparser.run_parser, options.worker)
    # ioLoop.add_callback(blockparser.test)
    ioLoop.start()
