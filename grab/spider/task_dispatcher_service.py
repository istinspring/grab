from threading import Thread, Event
import time

from six.moves.queue import Empty, Queue

from grab.spider.base_service import BaseService


class TaskDispatcherService(BaseService):
    def __init__(self, spider):
        self.input_queue = Queue()
        self.spider = spider
        self.worker = self.create_worker(self.worker_callback)
        self.register_workers(self.worker)

    def start(self):
        self.worker.start()

    def worker_callback(self, worker):
        while True:
            worker.process_pause_signal()
            try:
                item = self.input_queue.get(True, 0.1)
            except Empty:
                pass
            else:
                if len(item) == 2:
                    result, task = item
                    meta = {}
                else:
                    result, task, meta = item
                self.spider.process_service_result(result, task, meta)
