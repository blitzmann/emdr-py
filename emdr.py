#!/usr/bin/env python2
import logging
import simplejson
from xml.utils.iso8601 import parse
from datetime import datetime
import gevent
from gevent.pool import Pool
from gevent import monkey; gevent.monkey.patch_all()
import zmq.green as zmq
import zlib
import sys
import redis

# The maximum number of greenlet workers in the greenlet pool.
MAX_NUM_POOL_WORKERS = 500

DEBUG   = False          # prints out a bunch of info.
REGIONS = 'regions.json' # set this to a file containing JSON of regions or False for all regions
VERSION = 1              # Change every time format for Redis values changes

REQUEST_TIMEOUT = 1500  # 1.5s
RELAY_RETRIES   = 5     # retries per relay
MAX_RETRIES     = False # set to an int if you want to terminate script after x retries
RELAYS = [              # top relay will be primary
    'tcp://relay-us-east-1.eve-emdr.com:8050',
    'tcp://relay-us-central-1.eve-emdr.com:8050',
    'tcp://relay-us-west-1.eve-emdr.com:8050'
]

ORDERS  = True  # collect information on orders
HISTORY = False # not implemented

##////////////////
## END CONFIG
##////////////////

NUM_RELAYS = len(RELAYS);
redis = redis.StrictRedis(host='localhost', port=6379, db=0)

class Printer():
    """
    Print things to stdout on one line dynamically
    """
    def __init__(self,data):
        sys.stdout.write("\r\x1b[K"+data.__str__())
        sys.stdout.flush()

        
def main():
    global utd      # there should be a better way to do these than with globals
    
    retry_relay = 0 # retry count per relay
    retry_max   = 0 # retry count total
    relay       = 0 # current relay in use
    utd         = 0 # increments every time we have an already UTD cache
    cont        = True # flag to exit out of loop
    
    # Relay connection
    context = zmq.Context()
    client = context.socket(zmq.SUB)
    client.connect(RELAYS[relay])
    client.setsockopt(zmq.SUBSCRIBE, "") # Disable filtering.

    poll = zmq.Poller()
    poll.register(client, zmq.POLLIN)
    
    # We use a greenlet pool to cap the number of workers at a reasonable level.
    greenlet_pool = Pool(size=MAX_NUM_POOL_WORKERS) 
    
    print("Consumer daemon started, waiting for jobs...")
    print("Worker pool size: %d" % greenlet_pool.size)

    while cont:
        socks = dict(poll.poll(REQUEST_TIMEOUT))
        if socks.get(client) == zmq.POLLIN: # message recieved within timeout
            greenlet_pool.spawn(worker, client.recv())
            output = "%d/%d orders processed, %d skipped due to up-to-date cache" % \
                     (int(redis.get('emdr-total-saved')), \
                     int(redis.get('emdr-total-processed')), \
                     utd)
            Printer(output) # update output to stdout
            redis.incr('emdr-total-processed')
        else:
            print "\nW: No response from server, retrying... (",retry_relay," / ",RELAY_RETRIES,")"
            client.setsockopt(zmq.LINGER, 0)
            client.close()
            poll.unregister(client)
            
            retry_relay += 1
            retry_max   += 1
            
            if MAX_RETRIES is not False and retry_max == MAX_RETRIES:
                print "E: Server(s) seem to be offline, max attempts reached, terminating"
                cont = False
                break
            elif retry_relay == RELAY_RETRIES:  # we've reachec max retry attempts for this relay  
                if (relay == NUM_RELAYS-1): # if we're at te last relay loop around
                    print "E: Server seems to be offline, switching (looping)"
                    relay       = 0
                    retry_relay = 0
                else:
                    print "E: Server seems to be offline, switching"
                    relay      += 1
                    retry_relay = 0

            print "I: Reconnecting (relay: ",relay,")"
            client = context.socket(zmq.SUB)
            client.setsockopt(zmq.SUBSCRIBE, "")
            client.connect(RELAYS[relay])
            poll.register(client, zmq.POLLIN)

def worker(job_json):
    '''
    todo:   look into logging to files per type id
            recurse into rowsets: every feed is not necessarily 1 typeID (though it usually is)
    '''
    global utd;
    
    if REGIONS is not False:
        json_data  = open(REGIONS)
        regionDict = simplejson.load(json_data)
        json_data.close()
    
    # Receive raw market JSON strings.
    market_json = zlib.decompress(job_json);
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
                    utd += 1
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
            redis.incr('emdr-total-saved')
           

if __name__ == '__main__':
    main()