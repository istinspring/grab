import logging
from threading import Thread, Event


class ParserComponent(object):
    def __init__(self, spider, pool_size):
        self.spider = spider
        self.pool_size = pool_size

        self.parser_pool = []
        for _ in range(self.pool_size):
            is_parser_idle, th = self.start_parser_thread()
            self.parser_pool.append({
                'is_parser_idle': is_parser_idle,
                'thread': th,
            })

    def start_parser_thread(self):
        is_parser_idle = Event()
        # We start `run_process`
        # method in new semi-process (actually it is a thread)
        # Because the use `run_process` of main spider instance
        # all changes made in handlers are applied to main
        # spider instance, that allows to suppport deprecated
        # spiders that do not know about multiprocessing mode
        spider.is_parser_idle = is_parser_idle
        th = Thread(target=spider.run_parser)
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
