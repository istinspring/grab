import logging
from threading import Thread, Event


class ParserService(object):
    def __init__(self, spider, pool_size):
        self.spider = spider
        self.pool_size = pool_size

        self.parser_pool = []

    def start(self):
        for _ in range(self.pool_size):
            is_parser_idle, th = self.start_parser_thread()
            self.parser_pool.append({
                'is_parser_idle': is_parser_idle,
                'thread': th,
            })

    def start_parser_thread(self):
        is_parser_idle = Event()
        th = Thread(target=self.spider.run_parser, args=[is_parser_idle])
        th.daemon = True
        th.start()
        return is_parser_idle, th

    def check_pool_health(self):
        to_remove = []
        for th in self.parser_pool:
            if not th['thread'].is_alive():
                self.spider.stat.inc('parser-thread-restore')
                is_parser_idle, new_th = self.start_parser_thread()
                self.parser_pool.append({
                    'is_parser_idle': is_parser_idle,
                    'thread': new_th,
                })
                to_remove.append(th)
        for th in to_remove:
            self.parser_pool.remove(th)
