import argparse
import sys
import os
import csv
import multiprocessing
import time
from pymongo import MongoClient
from dotenv import load_dotenv
from queue import Queue
from math import ceil
from src.worker import Worker


# load and assign global/env variables.
load_dotenv()
DBINFO_SAVE_PATH = './assets/journalists_info.csv'
INPUT_PATH = './assets/journalists_info.csv'
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
VERBOSE_MODE = False
COLORS = [
    '\033[92m',   # Green
    '\033[93m',   # Yellow
    '\033[94m',   # Blue
    '\033[95m',   # Magenta
    '\033[96m',   # Cyan
    '\033[97m',   # White
    '\033[31m',   # Dark Red
    '\033[32m',   # Dark Green
    '\033[33m',   # Dark Yellow
    '\033[34m',   # Dark Blue
    '\033[35m',   # Dark Magenta
    '\033[36m',   # Dark Cyan
    '\033[37m',   # Light Gray
    '\033[90m',   # Dark Gray
]

def retrieve_info_from_database() -> None:
    """
    This function is responsible for getting journalists data from PressKit
    MongoDB database and saving it locally to ./assets/journalists_info.csv.
    """
    def close_connection(client):
        if client: client.close()
        print('Connection to database closed successfully.')
    def get_all_journalists(journalists_collection):
        for data in journalists_collection.find():
            if not data['twitter']:
                continue
            journalist = {
                        'name': data['name'],
                        'id': str(data['_id']),
                        'twitter': data['twitter'],
                        'curr_bio': data['bio']
                    }
            yield journalist
    print('Getting journalist data from MongoDB...')
    client = None
    file = open(DBINFO_SAVE_PATH, 'w', newline='', encoding='utf-8')
    writer = csv.DictWriter(file, fieldnames={'name','id','twitter','curr_bio'})
    writer.writeheader()
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        journalists_collection = client["presskit"]["journalist_data"]
        journalists_generator = get_all_journalists(journalists_collection)
        for journalist in journalists_generator:
            writer.writerow(journalist)
        print('Successfully retrieved all journalist information from MongoDB.')
    except Exception as e:
        print(f'Error while retrieving and saving data from MongoDB: {e}')
        file.close()
        close_connection(client)
        sys.exit(1)
    finally:
        file.close()
        close_connection(client)


def index_range(value) -> tuple:
    """
    This function is responsible for verifying user range input.
    """
    try:
        start, end = map(int, value.split('-'))
        if start > end:
            raise argparse.ArgumentTypeError('''Invalid range: {}-{}. Start should be less 
                                             than or equal to end.'''.format(start, end))
        return (start, end)
    except ValueError:
        raise argparse.ArgumentTypeError('Invalid range format. Should be start-end.')


def count_journalists():
    """
    This function counts the total number of journalists present in the input file.
    """
    def counter_generator():
        with open(INPUT_PATH, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            _ = next(csv_reader)
            for _ in csv_reader: yield None
    total_nums = 0
    for _ in counter_generator(): total_nums += 1
    return total_nums


def load_journalists() -> dict:
    """
    This generator function returns the dict of each journalists in the input file.
    """
    with open(INPUT_PATH, 'r', encoding='utf-8') as f:
        csv_reader = csv.reader(f)
        header = next(csv_reader)
        for row in csv_reader:
            row_dict = dict(zip(header, row))
            yield row_dict


def worker_process(process_id : int, verbose : bool, queue : multiprocessing.Queue):
    worker = Worker(process_id, queue, verbose, printclr=COLORS[process_id%len(COLORS)])
    worker.start()


def start_new_process(args, processes) -> multiprocessing.Process:
    process = multiprocessing.Process(target=worker_process, args=args)
    process.start()
    processes.append(process)
    return processes



if __name__ == '__main__':
    start_time = time.time()    # for measuring the time taken to finish entire process.

    # handle command line arguments.
    parser = argparse.ArgumentParser(prog='XBioScraper', description='XBioScraper.', )
    parser.add_argument('-g', '--getfromdb', dest='getfromdb', action='store_true', help='Retrieve latest data from database')
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Verbose mode')
    parser.add_argument('-i', '--input-path', dest='input_path', type=str, help='Specify the input path of journalists twitter information')
    parser.add_argument('-np', '--number-of-processes', dest='np', default=1, type=int, help='Specify the number of child processes you want to utilize')
    parser.add_argument('-r', '--range', dest='range', type=index_range, default=(0,9999999), help='Specify a numerical range of indexes that will be scraped')
    args = parser.parse_args()


    # apply command line arguments values.
    if args.getfromdb: retrieve_info_from_database()
    if args.input_path: INPUT_PATH = args.input_path
    VERBOSE_MODE = args.verbose
    start, end = args.range


    # break up workload and start multiprocessing.
    processes = []
    max_index = count_journalists()
    start, end = min(start, max_index), min(end, max_index)     # clamp range numbers
    num_of_jobs = end - start                                   # get total number of journalists that we need to go through.
    num_of_jobs_each_process = ceil(num_of_jobs / args.np)      # get number of journalists each process should handle.
    cur_index, cur_queue = -1, multiprocessing.Queue()
    for journalist in load_journalists():
        cur_index += 1
        if cur_index < start: continue
        if cur_index >= end: break
        cur_queue.put(journalist)
        if cur_queue.qsize() >= num_of_jobs_each_process:       # start new process.
            start_new_process((len(processes), VERBOSE_MODE, cur_queue), processes)
            cur_queue = multiprocessing.Queue()                 # reset queue.

    if cur_queue.qsize() != 0:                                  # start new process for leftover jobs...
        start_new_process((len(processes), VERBOSE_MODE, cur_queue), processes)
        cur_queue = multiprocessing.Queue()                     # reset queue.

    for process in processes: process.join()                    # wait for all processes to finish.


    # finishing up.
    end_time = time.time()                                      # for measuring runtime.
    elapsed_time = end_time - start_time
    print(f'''Done! Took {round(elapsed_time,2)} seconds to go through {num_of_jobs} journalists.
          Avg Time: {round(elapsed_time/num_of_jobs,2)}s per journalist.''')
    
    # sys.exit(0)
    os._exit(0)                                                 # otherwise program would not exit for some reason...