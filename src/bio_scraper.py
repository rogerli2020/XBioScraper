class BioScraper:
    def __init__(self) -> None:
        pass
    
    def start(self):
        while not self.queue.empty():
            cur_journalist = self.queue.get()