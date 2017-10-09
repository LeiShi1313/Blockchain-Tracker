import pymongo
import argparse
from dateutil.parser import parse
import time
import datetime

parser = argparse.ArgumentParser(description='Optional app description')
parser.add_argument('--mongo_url', type=str, default='localhost')
parser.add_argument('--mongo_port', type=int, default=27017)
parser.add_argument('--mongo_database', type=str, default='blockchain')
parser.add_argument('--mongo_collection', type=str, default='block')


class BlockData(object):
    def __init__(self, url, port, database, collection):
        self.conn = pymongo.Connection(url, port)
        self.db = self.conn[database][collection]

    def find_by_date(self, date):
        if isinstance(date, str):
            date = parse(date)
        date_tomorrow = date + datetime.timedelta(1)

        t1 = int(time.mktime(date.timetuple()))
        t2 = int(time.mktime(date_tomorrow.timetuple()))
        print date, date_tomorrow, t1, t2
        result = self.db.find({'timestamp': {'$gte': t1, '$lt': t2}}).sort('_id')
        for item in result:
            print item
        return result

def main():
    options = parser.parse_args()
    data = BlockData(options.mongo_url, options.mongo_port, options.mongo_database, options.mongo_collection)

    print data.find_by_date(datetime.date(2015, 9, 19)+datetime.timedelta(1))

if __name__ == '__main__':
    main()
