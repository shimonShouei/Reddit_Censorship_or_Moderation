from data_layer.data_layer_mongo import MongoDataLayer
from download.data_download import *

if __name__ == '__main__':
    conn_str = 'localhost:27017'
    dd = data_downloader(data_layer=MongoDataLayer(conn_str))
    dd.run(subreddit_name='wallstreetbets', year=2022, start_month=9, start_day=10, submission_kind_list=['post'], run_type='m')