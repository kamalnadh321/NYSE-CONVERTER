from dask import dataframe as dd
def main():
    print('file format conversion started')
    df = dd.read_csv(
        'data/nyse_all/nyse_data/NYSE*.txt.gz',
        names = ['ticker', 'track_date', 'open_price', 'low_price',
                 'high_price', 'close_price', 'volume'],
        blocksize= None         
    )
    print('data frame is created and will return in json format')
    df.to_json(
        'data/nyse_all/nyse_json/part-*.json.gz',
        orient= 'records',
        lines= True,
        compression  = 'gzip',
        name_function= lambda i: '%05d' % i
    )
    print('file format conversion completely')
    
if __name__ == '__main__':
    main()