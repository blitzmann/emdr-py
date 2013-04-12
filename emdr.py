#!/usr/bin/env python2
# This can be replaced with the built-in json module, if desired.
import logging
import simplejson
import json
from xml.utils.iso8601 import parse
import datetime
import MySQLdb
from datetime import datetime
import gevent
from gevent.pool import Pool
from gevent import monkey; gevent.monkey.patch_all()
import zmq.green as zmq
import zlib
import pylibmc
import operator
import sys
import redis

# The maximum number of greenlet workers in the greenlet pool. This is not one
# per processor, a decent machine can support hundreds or thousands of greenlets.
# I recommend setting this to the maximum number of connections your database
# backend can accept, if you must open one connection per save op.
MAX_NUM_POOL_WORKERS = 500
DEBUG = False
REGIONS = 'regions.json' #set this to a file containing JSON of regions to filter against
VERSION = 1 # Change every time format for Redis values changes

ORDERS  = True
HISTORY = False # not implemented

## todo: customizable key format
redis = redis.StrictRedis(host='localhost', port=6379, db=0)

class Printer():
    """
    Print things to stdout on one line dynamically
    """
 
    def __init__(self,data):
        sys.stdout.write("\r\x1b[K"+data.__str__())
        sys.stdout.flush()

        
def main():
    """
    The main flow of the application.
    """

    global f
    global s
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    # Connect to the first publicly available relay.
    subscriber.connect('tcp://relay-us-east-1.eve-emdr.com:8050')

    # Disable filtering.
    subscriber.setsockopt(zmq.SUBSCRIBE, "")

    # We use a greenlet pool to cap the number of workers at a reasonable level.
    greenlet_pool = Pool(size=MAX_NUM_POOL_WORKERS) 
    
    print("Consumer daemon started, waiting for jobs...")
    print("Worker pool size: %d" % greenlet_pool.size)
    i = f = s = 0
    while True:
        # Since subscriber.recv() blocks when no messages are available,
        # this loop stays under control. If something is available and the
        # greenlet pool has greenlets available for use, work gets done.
        greenlet_pool.spawn(worker, subscriber.recv())
        output = "%d/%d orders processed, %d skipped due to up-to-date cache" % (f,i,s)
        Printer(output)
        i += 1
    
def worker(job_json):
    """
    For every incoming message, this worker function is called. Be extremely
    careful not to do anything CPU-intensive here, or you will see blocking.
    Sockets are async under gevent, so those are fair game.
    """
    
    '''
    todo:   look into putting it into mysql, loading mysql into Redis
            look into logging to files per type id
            recurse into rowsets: every feed is not necessarily 1 typeID (though it usually is)
    '''
    global f;
    global s;
    
    if REGIONS is not False:
        json_data = open(REGIONS)
        regionDict   = json.load(json_data)
        json_data.close()
    else:
        pass
    
    # Receive raw market JSON strings.
    market_json = zlib.decompress(job_json);

    # Un-serialize the JSON data to a Python dict.
    market_data = simplejson.loads(market_json);
    
    # Gather some useful information
    name = market_data.get('generator');
    name = name['name'];
    resultType = market_data.get('resultType');
    rowsets = market_data.get('rowsets')[0]; # todo: recurse into others, not just [0]
    typeID = rowsets['typeID'];
    columns = market_data.get('columns');
    
    # Convert str time to int
    currentTime = parse(market_data.get('currentTime'));
    generatedAt = parse(rowsets['generatedAt']);
    
    numberOfSellItems = 0;
    numberOfBuyItems  = 0;

    sellPrice = {}
    buyPrice  = {}
    data = { # set defaults, will be overwritten during parsing, or use old cache?
        'orders': {
            'generatedAt': False,
            'sell': False,
            'buy': False
        },
        'history': {
            'generatedAt': False}
    }
        
    if DEBUG: # write raw json to file
        try:
            file = open("type-"+str(typeID)+".txt", "w")
            try:
                file.write(market_json) # Write a string to a file
            finally:
                file.close()
        except IOError:
            pass
        
    '''
    Cache is in this format: 
    
    emdr-VERSION-REGIONID-TYPEID = 
        {'orders': {
            'generatedAt': timestamp,
            'sell': [fiveAverageSellPrice, numberOfSellItems],
            'buy': [fiveAverageBuyPrice, numberOfBuyItems] }
        'history': [] }
    '''
   
    if (REGIONS == False or (REGIONS != False and str(rowsets['regionID']) in regionDict)):
        cached = redis.get('emdr-'+str(VERSION)+'-'+str(rowsets['regionID'])+'-'+str(typeID));
        
        # If data has been cached for this item, check the dates. If dates match, skip
        # todo: TEST TO MAKE SURE this is not deleting data, and only overwriting old cache
        if (cached != None):
            #if we have data in cache, split info into list
            cache = simplejson.loads(cached);
            if DEBUG:
                print "\n\n(",resultType,") Cache:",cache
            # parse date
            if cache[resultType]['generatedAt'] is not False:
                cachedate = cache[resultType]['generatedAt'];
                if (DEBUG):
                        print "\nCached data found (result type: ",resultType,")!\n\tNew date: "+str(datetime.fromtimestamp(generatedAt))+"\n\tCached date: "+ str(datetime.fromtimestamp(cachedate));
                if (generatedAt < cachedate):
                    s += 1
                    if (DEBUG):
                        print "\t\tSKIPPING";
                    return '';
                
            data = cache # set default data to cached data
                
        if (ORDERS and resultType == 'orders'):
            data['orders']['generatedAt'] = generatedAt
            if (DEBUG):
                print "\n\n\n\n======== New record ========";

            # Start putting pricing info (keys) into dicts with volume (values)
            for row in rowsets['rows']:
                order = dict(zip(columns, row))
                
                if (order['bid'] == False):
                    if (DEBUG):
                        print "Found sell order for "+str(order['price']) + "; vol: "+str(order['volRemaining']);
                    if (sellPrice.get(order['price']) != None):
                        sellPrice[order['price']] += order['volRemaining'];
                    else:
                        sellPrice[order['price']] = order['volRemaining'];
                    numberOfSellItems += order['volRemaining'];
                else:
                    if (DEBUG):
                        print "Found buy order for "+str(order['price']) + "; vol: "+str(order['volRemaining']);
                    if (buyPrice.get(order['price']) != None):
                        buyPrice[order['price']] += order['volRemaining'];
                    else:
                        buyPrice[order['price']] = order['volRemaining'];
                    numberOfBuyItems += order['volRemaining'];
                #end loop
                
            if (DEBUG):   
                print "\nSell dict:",sellPrice
                print "\nBuy dict:",buyPrice
                print "\nTotal volume on market: ",numberOfSellItems," Sell + ",numberOfBuyItems
            
            if (numberOfSellItems > 0):
                prices = sorted(sellPrice.items(), key=lambda x: x[0]);
                fivePercentOfTotal = max(int(numberOfSellItems*0.05),1);
                fivePercentPrice=0;
                bought=0;
                boughtPrice=0;
                if (DEBUG):
                    print "Sell Prices (sorted):\n",prices
                    print "Start buying process!"
                while (bought < fivePercentOfTotal):
                    pop = prices.pop(0)
                    fivePercentPrice = pop[0]
                    if (DEBUG):
                        print "\tBought: ",bought,"/",fivePercentOfTotal
                        print "\t\tNext pop: ",fivePercentPrice," ISK, vol: ",pop[1]
                    
                    if (fivePercentOfTotal > ( bought + sellPrice[fivePercentPrice])):
                        boughtPrice += sellPrice[fivePercentPrice]*fivePercentPrice;
                        bought += sellPrice[fivePercentPrice];
                        if (DEBUG):
                            print "\t\tHave not met goal. Bought:",bought
                    else:
                        diff = fivePercentOfTotal - bought;
                        boughtPrice += fivePercentPrice*diff;
                        bought = fivePercentOfTotal;
                        if (DEBUG):
                            print "\t\tGoal met. Bought:",bought
                
                fiveAverageSellPrice = boughtPrice/bought;
                if (DEBUG):
                    print "Average selling price (first 5% of volume):",fiveAverageSellPrice
                data['orders']['sell'] = [
                    "%.2f" % fiveAverageSellPrice,
                    numberOfSellItems]

            if (numberOfBuyItems > 0):
                prices = sorted(buyPrice.items(), key=lambda x: x[0], reverse=True);
                fivePercentOfTotal = max(int(numberOfBuyItems*0.05),1);
                fivePercentPrice=0;
                bought=0;
                boughtPrice=0;
                if (DEBUG):
                    print "Buy Prices (sorted):\n",prices
                    print "Start buying process!"
                while (bought < fivePercentOfTotal):
                    pop = prices.pop(0)
                    fivePercentPrice = pop[0]
                    if (DEBUG):
                        print "\tBought: ",bought,"/",fivePercentOfTotal
                        print "\t\tNext pop: ",fivePercentPrice," ISK, vol: ",pop[1]
                    
                    if (fivePercentOfTotal > ( bought + buyPrice[fivePercentPrice])):
                        boughtPrice += buyPrice[fivePercentPrice]*fivePercentPrice;
                        bought += buyPrice[fivePercentPrice];
                        if (DEBUG):
                            print "\t\tHave not met goal. Bought:",bought
                    else:
                        diff = fivePercentOfTotal - bought;
                        boughtPrice += fivePercentPrice*diff;
                        bought = fivePercentOfTotal;
                        if (DEBUG):
                            print "\t\tGoal met. Bought:",bought
                
                fiveAverageBuyPrice = boughtPrice/bought;
                if (DEBUG):
                    print "Average buying price (first 5% of volume):",fiveAverageBuyPrice
                data['orders']['buy'] = [
                    "%.2f" % fiveAverageBuyPrice,
                    numberOfBuyItems]
            
            redis.set('emdr-'+str(VERSION)+'-'+str(rowsets['regionID'])+'-'+str(typeID), simplejson.dumps(data));
            if (DEBUG):
                print 'SUCCESS: emdr-'+str(VERSION)+'-'+str(rowsets['regionID'])+'-'+str(typeID),simplejson.dumps(data)
            f += 1
           

if __name__ == '__main__':
    main()