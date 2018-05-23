#!/usr/bin/env python

import argparse
import common.config
import common.args
from datetime import datetime
from dateutil.relativedelta import relativedelta
import dateutil.parser
from v20.instrument import Candlestick
import time

def price_to_csv(data):
    return ','.join([str(data.o),str(data.h),str(data.l),str(data.c)])
    
def candle_to_csv(candle):
    return ','.join([candle.time, str(candle.volume), str(candle.complete), price_to_csv(candle.bid), price_to_csv(candle.ask), price_to_csv(candle.mid)])

def price_from_csv(str):
    data = str.split(',')
    return Candlestick.from_dict({'time':data[0], 'volume':data[1], 'complete':data[2],\
        'bid': {'o':data[3], 'h':data[4], 'l':data[5], 'c':data[6]},\
        'ask': {'o':data[3], 'h':data[4], 'l':data[5], 'c':data[6]},\
        'mid': {'o':data[3], 'h':data[4], 'l':data[5], 'c':data[6]}})
    
def main():
    """
    Create an API context, and use it to fetch historical data for multiple instruments 
    and saves it to S3 or a local file system

    The configuration for the context is parsed from the config file provided
    as an argumentV
    """

    parser = argparse.ArgumentParser()

    #
    # The config object is initialized by the argument parser, and contains
    # the REST APID host, port, accountID, etc.
    #
    common.config.add_argument(parser)

    parser.add_argument(
        "instruments",
        type=common.args.instrument,
        help="The instruments to get candles for"
    )

    parser.add_argument(
        "--mid", 
        action='store_true',
        default=True,
        help="Get midpoint-based candles"
    )

    parser.add_argument(
        "--bid", 
        action='store_true',
        default=True,
        help="Get bid-based candles"
    )

    parser.add_argument(
        "--ask", 
        action='store_true',
        default=True,
        help="Get ask-based candles"
    )

    parser.add_argument(
        "--smooth", 
        action='store_true',
        help="'Smooth' the candles"
    )

    parser.set_defaults(mid=False, bid=False, ask=False)

    parser.add_argument(
        "--granularity",
        default=None,
        help="The candles granularity to fetch"
    )

    date_format = "%Y-%m-%d %H:%M:%S"

    parser.add_argument(
        "--from-time",
        default=None,
        type=common.args.date_time(),
        help="The start date for the candles to be fetched. Format is 'YYYY-MM-DD HH:MM:SS'"
    )

    parser.add_argument(
        "--to-time",
        default=None,
        type=common.args.date_time(),
        help="The end date for the candles to be fetched. Format is 'YYYY-MM-DD HH:MM:SS'"
    )
    
    parser.add_argument(
        "--hourly",
        action='store_true',
        help="store hourly batch files instead of daily"
    )

    parser.add_argument(
        "--alignment-timezone",
        default=None,
        help="The timezone to used for aligning daily candles"
    )

    args = parser.parse_args()

    account_id = args.config.active_account

    #
    # The v20 config object creates the v20.Context for us based on the
    # contents of the config file.
    #
    api = args.config.create_context()
    output = args.config.create_storage_context()

    kwargs = {}

    if args.granularity is not None:
        kwargs["granularity"] = args.granularity

    if args.smooth is not None:
        kwargs["smooth"] = args.smooth

    if args.alignment_timezone is not None:
        kwargs["alignmentTimezone"] = args.alignment_timezone

    if args.mid:
        kwargs["price"] = "M" + kwargs.get("price", "")
        price = "mid"

    if args.bid:
        kwargs["price"] = "B" + kwargs.get("price", "")
        price = "bid"

    if args.ask:
        kwargs["price"] = "A" + kwargs.get("price", "")
        price = "ask"
   
    # function to iteratively query candles for range
    last_request = time.clock() - 1
    def query_candles(instrument, begin, end, time_inc): 
        wait_time = last_request + .25 - time.clock()
        if wait_time > 0:
            time.sleep(wait_time)
            
        start = begin
        finish = None
        while finish is None or finish != end:
            finish = start + time_inc
            if finish > end:
                finish = end
            
            kwargs["fromTime"] = api.datetime_to_str(start)
            kwargs["toTime"] = api.datetime_to_str(finish)
            start = finish
            response = api.instrument.candles(instrument, **kwargs)
            
            count = 0
            try_again = True
            while try_again:
                try:
                    candles = response.get("candles", 200)
                    try_again = False
                    break;
                    
                except Exception as e:
                    print(e)
                    
                    if count < 5:
                        print("retrying")
                        count = count + 1 
                    else:
                        print("failing after {} tries".format(count))
                        try_again = False
                        
                    
                    
                    
                
            for candle in candles:
                yield candle
            
   
    def write_batch(hourly, begin, instrument, batch, output):
        if hourly:
            key =  "oanda/{:02d}/{:02d}/{:02d}/{:02d}/{}_CANDLES_{}.csv".format(begin.year, begin.month, begin.day, begin.hour, instrument, args.granularity)
            print('writing', str(len(batch)), 'lines to', key)
            output.write(key, '\n'.join(batch))
        else:
            key =  "oanda/{:02d}/{:02d}/{:02d}/{}_CANDLES_{}.csv".format(begin.year, begin.month, begin.day, instrument, args.granularity)
            print('writing', str(len(batch)), 'lines to', key)
            output.write(key, '\n'.join(batch)) 
            
    # get the begin and end
    if args.to_time:
        end = args.to_time
    else:
        end = datetime.now()
        
    if args.from_time:
        begin = args.from_time
    else:
        begin = end - relativedelta(days=1)
    
    # calcualte the frequency
    frequency = {'H1':24, 'M1':1440, 'S30': 2880, 'S15': 5760, 'S5': 17280}[args.granularity]
    time_inc =  relativedelta(days=2880/frequency) 
    
    instruments = args.instruments.split(',')
    for instrument in instruments:
        # now create day or hour batch files   
        batch = []
        previous_batch = None
        for candle in query_candles(instrument, begin, end, time_inc):
            candletime = dateutil.parser.parse(candle.time)
            if args.hourly:
                current_batch = datetime(year=candletime.year, month=candletime.month, day=candletime.day, hour=candletime.hour)
            else:
                current_batch = datetime(year=candletime.year, month=candletime.month, day=candletime.day)
            
            if previous_batch is None:
                previous_batch = current_batch
                
            if previous_batch is None or previous_batch == current_batch:
                batch.append(candle_to_csv(candle))
            else:
                write_batch(args.hourly, current_batch, instrument, batch, output)
                batch = []   
                previous_batch = current_batch
        
        if len(batch) > 0:
            write_batch(args.hourly, current_batch, instrument, batch, output)   

if __name__ == "__main__":
    main()
