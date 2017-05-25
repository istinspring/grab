from threading import Event, Thread
import time

from six.moves.queue import Queue, Empty

from grab.spider.base_service import BaseService


class CacheServiceBase(BaseService):
    def __init__(self, spider, backend):
        self.spider = spider
        self.backend = backend
        self.queue_size_limit = 100
        self.input_queue = Queue()
        self.worker = self.create_worker(self.worker_callback)
        self.register_workers(self.worker)

    def shutdown(self):
        try:
            self.backend.close()
        except AttributeError:
            pass


class CacheReaderService(CacheServiceBase):
    def worker_callback(self, worker):
        try:
            while not worker.stop_event.is_set():
                worker.process_pause_signal()
                try:
                    task = self.input_queue.get(True, 0.1)
                except Empty:
                    pass
                else:
                    grab = self.spider.setup_grab_for_task(task)
                    result = None
                    if self.is_read_allowed(task, grab):
                        item = self.load_from_cache(task, grab)
                    if item:
                        result, task = item
                        self.spider.task_dispatcher.input_queue.put(
                            (result, task)
                        )
                    else:
                        #self.spider.add_task(
                        #    task, queue=self.spider.task_queue,
                        #)
                        self.spider.task_dispatcher.input_queue.put((
                            task, None, {'source': 'cache_reader'}
                        ))
        finally:
            self.shutdown()

    def is_read_allowed(self, task, grab):
        return (
            not task.get('refresh_cache', False)
            and not task.get('disable_cache', False)
            and grab.detect_request_method() == 'GET'
        )

    def load_from_cache(self, task, grab):
        cache_item = self.backend.get_item(
            grab.config['url'], timeout=task.cache_timeout)
        if cache_item is None:
            return None
        else:
            grab.prepare_request()
            self.backend.load_response(grab, cache_item)
            grab.log_request('CACHED')
            self.spider.stat.inc('spider:request-cache')

            return {
                'ok': True,
                'grab': grab,
                'grab_config_backup': grab.dump_config(),
                'emsg': None,
            }, task


class CacheWriterService(CacheServiceBase):
    def worker_callback(self, worker):
        try:
            while not worker.stop_event.is_set():
                worker.process_pause_signal()
                try:
                    task, grab = self.input_queue.get(True, 0.1)
                except Empty:
                    pass
                else:
                    if self.is_write_allowed(task, grab):
                        self.backend.save_response(task.url, grab)
        finally:
            self.shutdown()

    def is_write_allowed(self, task, grab):
        return ( 
            grab.request_method == 'GET'
            and not task.get('disable_cache')
            and self.spider.is_valid_network_response_code(
                grab.doc.code, task
            )
        )
