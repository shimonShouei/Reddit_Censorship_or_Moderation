import datetime
import logging
import os
import time
import calendar
import datetime
import asyncio
import time
import logging
import praw
from requests.exceptions import ChunkedEncodingError
from pmaw import PushshiftAPI
from tqdm import tqdm
from dotenv import load_dotenv

logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s %(message)s')





class data_downloader:
    def __init__(self, data_layer, chunk_size = 1000):
        load_dotenv()
        self.__chunk_size = chunk_size
        self.__chunk_counter = self.__chunk_size
        self.__curr_tmp_chunk = {}
        self.__curr_chunk = {}
        self.__times_praw = [0, 0]
        self.__times_psaw = [0, 0]
        self.pushshift_api = PushshiftAPI(num_workers=12)
        self.reddit_api = praw.Reddit(
            client_id=os.getenv('CLIENT_ID'),
            client_secret=os.getenv('CLIENT_SECRET'),
            user_agent=os.getenv('USER_AGENT'),
            username=os.getenv('USER_NAME'),
            password=os.getenv('PASSWORD'),
            check_for_async=False
        )
        self.__download_funcs_dict = {'post': self.pushshift_api.search_submissions, 'comment': self.pushshift_api.search_comments}
        self.data_layer = data_layer

    def convert_time_format(self, comment_or_post):
        comment_or_post['created_utc'] = datetime.datetime.fromtimestamp(
            comment_or_post['created_utc']).isoformat().split(
            "T")
            
    def define_status(self, p):
        status = ''
        try:
            reddit_post = p["reddit_api"]  # ["post"]
        except KeyError as e:
            reddit_post = p["pushift_api"]
        # if "bot_comment" in p:
        #     status = "automod_filtered"
        if not reddit_post["is_robot_indexable"]:
            reddit_selftext = reddit_post["selftext"].lower()
            if reddit_post["removed_by_category"] == "automod_filtered":
                status = "automod_filtered"
            elif reddit_selftext == "[removed]":
                status = "removed"
            elif reddit_selftext.__contains__("[removed]") and reddit_selftext.__contains__("poll"):
                status = "poll"
            elif reddit_post["removed_by_category"] in ["[deleted]", "[deleted by user]"] or reddit_selftext in [
                "[deleted]", "[deleted by user]"]:
                status = "deleted"
            else:
                status = "removed"
        else:
            status = "exist"
        return status

    async def __handle_single_submission(self, sub, kind, _post_or_comment, pbar_):
        s_time = time.time()
        # try:
        if '_reddit' in sub.keys():
            del sub["_reddit"]
        if 'subreddit' in sub.keys():
            del sub["subreddit"]
        if 'author' in sub.keys():
            del sub["author"]  # TODO keep author (check)
        if 'poll_data' in sub.keys():
            sub['poll_data'] = str(sub['poll_data'])
        self.convert_time_format(sub)
        post_id = sub["id"]
        if kind == "reddit_api":
            if post_id in self.__curr_tmp_chunk:
                for k in self.__curr_tmp_chunk[post_id]["pushift_api"].copy():
                    if k in sub:
                        if sub[k] == self.__curr_tmp_chunk[post_id]["pushift_api"][k]:
                            del self.__curr_tmp_chunk[post_id]["pushift_api"][k]
            else:
                self.__curr_tmp_chunk[post_id] = {}
                self.__curr_tmp_chunk[post_id]["post_id"] = post_id
            sub = dict(sorted(sub.items(), key=lambda item: item[0]))
            self.__curr_tmp_chunk[post_id][kind] = sub
            e_time = time.time()
            self.__times_praw[0] += (e_time - s_time)
            self.__times_praw[1] += 1
        else:
            if post_id in self.__curr_tmp_chunk:
                for k in sub.copy():
                    if k in self.__curr_tmp_chunk[post_id]["reddit_api"]:
                        if sub[k] == self.__curr_tmp_chunk[post_id]["reddit_api"][k]:
                            del sub[k]
            else:
                self.__curr_tmp_chunk[post_id] = {}
                self.__curr_tmp_chunk[post_id]["post_id"] = post_id
            self.__curr_tmp_chunk[post_id][kind] = sub
            e_time = time.time()
            self.__times_psaw[0] += (e_time - s_time)
            self.__times_psaw[1] += 1
        if len(self.__curr_tmp_chunk[post_id]) == 3:
            status = self.define_status(self.__curr_tmp_chunk[post_id])
            self.__curr_tmp_chunk[post_id]['status'] = status
            self.__curr_chunk[post_id] = self.__curr_tmp_chunk[post_id].copy()
            del self.__curr_tmp_chunk[post_id]
            self.__chunk_counter -= 1
        pbar_.update(1)

        if self.__chunk_counter <= 0:
            self.__chunk_counter = self.__chunk_size
            start_time_dump = time.time()
            await self.__dump_data()
            pbar_.reset()
            logging.info("\nwrite to db duration: {}.".format(time.time() - start_time_dump))

    async def __dump_data(self):
        await self.data_layer.insert_many(data=self.__curr_chunk.values())
        self.__curr_chunk = {}

    async def __handle_submissions(self, submissions_list, _post_or_comment, api_kind):
        with tqdm(total=len(submissions_list)) as pbar:
            await asyncio.gather(
                *[self.__handle_single_submission(submission, api_kind, _post_or_comment, pbar) for submission in
                  submissions_list])


    async def __download(self, d, m, year, sub_kind, subreddit_name, last_day_of_month, run_type='m'):
        start_time = int(datetime.datetime(year, m, d, 0, 0).timestamp())
        if start_time > int(datetime.datetime.now().timestamp()):
            return
        if run_type == "m":
            d = last_day_of_month
        end_time = int(datetime.datetime(year, m, d, 23, 59).timestamp())
        logging.info(f"start date:{d}/{m}/{year}")
        submissions_list_pushift = []
        submissions_list_reddit = []
        start_run_time = time.time()
        # loop = asyncio.get_event_loop()
        try:
            submissions_list_pushift = self.__download_funcs_dict.get(sub_kind)(subreddit=subreddit_name,
                                                                     after=start_time,
                                                                     before=end_time, safe_exit=True)
            await self.__change_reddit_mode(),
            submissions_list_reddit = self.__download_funcs_dict.get(sub_kind)(subreddit=subreddit_name,
                                                                     after=start_time,
                                                                     before=end_time)
            await self.__change_reddit_mode(),
            end_run_time = time.time()
            await asyncio.gather(
            self.__handle_submissions(submissions_list_pushift, sub_kind, "pushift_api"),
            # logging.info("Extract from pushift time: {}".format(end_run_time - start_run_time))
            
            self.__handle_submissions(submissions_list_reddit, sub_kind, "reddit_api")
            )
            # logging.info("Extract from reddit time: {}".format(time.time() - end_run_time))
        except ChunkedEncodingError as e:
            logging.warn(f"Error at {d}/{m}/{year}")
        if len(self.__curr_tmp_chunk) > 0:
            self.__curr_chunk.update(self.__curr_tmp_chunk)
            self.__curr_tmp_chunk = {}
        logging.info(f"Mean time to handle reddit {sub_kind} is: {self.__times_praw[0] / self.__times_praw[1]}")
        logging.info(
            f"Mean time to handle pushift {sub_kind} is: {self.__times_praw[0] / self.__times_praw[1]}")

    async def __change_reddit_mode(self):
        if self.pushshift_api.praw:
            self.pushshift_api.praw = None
        else:
            self.pushshift_api.praw = self.reddit_api
    
    def run(self, subreddit_name, year, submission_kind_list, start_day=1, start_month=1, m_step=1,
                           d_step=1, run_type='m'):

        for sub_kind in submission_kind_list:
            logging.info(f"Downloading {sub_kind}s")
            mycol = self.data_layer.get_collection(year, subreddit_name, sub_kind)
            index_name = 'pid'
            if index_name not in mycol.index_information():
                mycol.create_index([('post_id', 1)], name=index_name, unique=True)
            if sub_kind == "comment":
                run_type = "d"
            loop = asyncio.get_event_loop()
            for m in range(start_month, 13, m_step):
                last_day_of_month = calendar.monthrange(year, m)[1]
                logging.info(f'last day of month {m} is {last_day_of_month}')
                first_day = 1
                if m == start_month:
                    first_day = start_day
                if run_type == "m":
                        d_step = last_day_of_month
                loop.run_until_complete(asyncio.gather(*[self.__download(d=d, m=m, sub_kind=sub_kind, subreddit_name=subreddit_name, year=year, last_day_of_month=last_day_of_month, run_type=run_type)
                                                         for d in range(first_day, last_day_of_month + 1, d_step)]))
                # for d in range(first_day, last_day_of_month + 1, d_step):
                #     loop = self.__download(d=d, m=m, sub_kind=sub_kind, subreddit_name=subreddit_name, year=year, last_day_of_month=last_day_of_month, run_type=run_type)
            # empty chunk
            if len(self.__curr_chunk) > 0:
                s_time_dump = time.time()
                loop.run_until_complete(self.__dump_data())
                logging.info("Last write time: {}.".format(time.time() - s_time_dump))