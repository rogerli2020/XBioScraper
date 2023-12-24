import multiprocessing

class Worker:
    def __init__(self, process_id : int, queue : multiprocessing.Queue, verbose : bool=False, printclr : str='') -> None:
        self.process_id = process_id
        self.queue = queue
        self.verbose = verbose
        self.printclr = printclr
        self.end_color = '\033[0m'
        self.total = self.queue.qsize()

    def colored_print(self, s : str) -> None:
        print(f'{self.printclr}Process {self.process_id}: \t{s}{self.end_color}')

    def start(self):
        self.colored_print(f'Process started with {self.total} jobs...')
        count = 1
        while not self.queue.empty():
            cur_journalist = self.queue.get()
            self.colored_print(f"({count}/{self.total}) Processing {cur_journalist['name']}...")
