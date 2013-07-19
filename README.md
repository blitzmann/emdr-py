emdr-py
========================
emdr-py is an [EMDR](https://eve-market-data-relay.readthedocs.org/en/latest/) consumer script written in Python. It is a set-and-forget script, only needing to be restarted should the process quit unexpectidely. It gathers pricing information from the EMDR network 24/7 and stores a calculated value in a Redis key-value datbase.

NOTE: emdr-memcache.py is for archival purposes, as this is the script I originally used before switching to Redis. It will not be updated with tweaks/fixes. There may be a time when I can merge both and the option of switching to either one depending on the client's needs.

Requirements
============
* Python >= 2.6
* [Redis](https://github.com/andymccurdy/redis-py)
* High bandwidth (tested at a constant 245kbps, although this rate can increase/decrease based on contributiones to the EMDR network)

Features
=========
* Uses Redis in-memory storage of values for fast read/write that can be used in any app with a Redis library
* Automatically reconnects to EMDR network if connection is lost
* Calculates a price for each item (based on EVE Centrals 5% percentile)

Run
=========
Download the source.
```
git clone https://github.com/blitzmann/emdr-py.git
cd emder-py
```

Edit config values in emdr.py and regions.json

Start the app in the background
```
screen python emdr.py
```

How is pricing calculated
=========
emdr-py simulates a 5% purchase of the total volume, and averages out the cost it took to do so. This is identical to how EVE Central calculates it's percentile value, and helps to avoid outliers.

Redis values
=========
Redis keys are of the following format: ```emdr-VERSION-REGIONID-TYPEID```. If for whatever reason the format of the values changes, the version number is incremented. This is to avoid breaking applications when the format changes.

Redis values are in this format: 

    {
    'orders': {
        'generatedAt': timestamp,
        'sell': [fivePercentSellPrice, toalSellItems],
        'buy': [fivePercentBuyPrice, totalBuyItems] 
    	}
    'history': [NOT IMPLEMENTED] 
    }
        
        
