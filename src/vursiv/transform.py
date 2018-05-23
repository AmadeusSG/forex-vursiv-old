

def file_key(org, instrument, type, granularity, start, hourly=False):
    if hourly:
        return  "{}/{:02d}/{:02d}/{:02d}/{:02d}/{}_{}_{}.csv".format(org, begin.year, begin.month, begin.day, begin.hour, instrument, type, granularity)
    else:
        return  "{}/{:02d}/{:02d}/{:02d}/{}_{}_{}.csv".format(org, begin.year, begin.month, begin.day, instrument, type, granularity)

def get_data(instruments, start, count):
    key = "onda"
    

def features_from_candle_csv(candle):
    
    
    
    
    
    
    