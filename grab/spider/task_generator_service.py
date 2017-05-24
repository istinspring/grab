import time
from threading import Thread, Event

import six


class TaskGeneratorService(object):
    def __init__(self, real_generator, spider):
        self.real_generator = real_generator
        self.spider = spider
        self.pause_event = Event()
        self.resume_event = Event()
        self.activity_paused = Event()

        self.thread = Thread(target=self.thread_worker)
        self.thread.daemon = True

    def start(self):
        self.thread.start()

    def is_alive(self):
        return self.thread.isAlive()

    def pause(self):
        self.resume_event.clear()
        self.pause_event.set()
        self.activity_paused.wait()

    def resume(self):
        self.pause_event.clear()
        self.activity_paused.clear()
        self.resume_event.set()

    def thread_worker(self):
        # TODO: use one intermediary task qeueue to collect tasks
        # from all task generators
        # That queue have to be limited by size ( thread_number * 10 or
        # something like that)
        # Probably that should NOT be related to number of threads
        # That would help to get rid of time.sleep(0.1) because
        # it would blocks in `queue.put` point
        while True:
            if self.pause_event.is_set():
                self.activity_paused.set()
                self.resume_event.wait()
            queue_size = self.spider.task_queue.size()
            min_limit = self.spider.thread_number * 10
            if queue_size < min_limit:
                try:
                    for _ in six.moves.range(min_limit - queue_size):
                        if self.pause_event.is_set():
                            break
                        item = next(self.real_generator)
                        self.spider.process_handler_result(item, None)
                except StopIteration:
                    break
            else:
                time.sleep(0.1)
