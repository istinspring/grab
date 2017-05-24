from threading import Event, Thread
import time

from six.moves.queue import Queue, Empty


class CacheService(object):
    def __init__(self, spider, backend):
        self.spider = spider
        self.backend = backend
        self.queue_size = 100
        self.input_queue = Queue()
        self.result_queue = Queue()
        self.is_finished = False

        self.thread = Thread(target=self.thread_worker)
        self.thread.daemon = True

        self.pause_event = Event()
        self.resume_event = Event()
        self.activity_paused = Event()

    def start(self):
        self.thread.start()

    def pause(self):
        self.resume_event.clear()
        self.pause_event.set()
        self.activity_paused.wait()

    def resume(self):
        self.pause_event.clear()
        self.activity_paused.clear()
        self.resume_event.set()

    def has_free_resources(self):
        return (self.input_queue.qsize() < self.queue_size
                and self.result_queue.qsize() < self.queue_size)

    def is_idle(self):
        return (
            not self.input_queue.qsize()
            and not self.input_queue.qsize()
        )

    def thread_worker(self):
        try:
            while True:
                if self.pause_event.is_set():
                    self.activity_paused.set()
                    self.resume_event.wait()
                try:
                    action, data = self.input_queue.get(True, 0.1)
                except Empty:
                    if self.spider.shutdown_event.is_set():
                        return
                else:
                    assert action in ('load', 'save', 'pause')
                    if action == 'load':
                        task, grab = data
                        result = None
                        if self.is_cache_loading_allowed(task, grab):
                            result = self.load_from_cache(task, grab)
                        if result:
                            self.result_queue.put(('network_result', result))
                        else:
                            self.result_queue.put(('task', task))
                    elif action == 'save':
                        task, grab = data
                        if self.is_cache_saving_allowed(task, grab):
                            self.backend.save_response(task.url, grab)
        finally:
            self.shutdown()

    def is_cache_loading_allowed(self, task, grab):
        # 1) cache data should be refreshed
        # 2) cache is disabled for that task
        # 3) request type is not cacheable
        return (not task.get('refresh_cache', False)
                and not task.get('disable_cache', False)
                and grab.detect_request_method() == 'GET')

    def is_cache_saving_allowed(self, task, grab):
        """
        Check if network transport result could
        be saved to cache layer.

        res: {ok, grab, grab_config_backup, task, emsg}
        """

        if grab.request_method == 'GET':
            if not task.get('disable_cache'):
                if self.spider.is_valid_network_response_code(grab.doc.code,
                                                              task):
                    return True
        return False

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

            return {'ok': True, 'task': task, 'grab': grab,
                    'grab_config_backup': grab.dump_config(),
                    'emsg': None}

    def shutdown(self):
        self.is_finished = True
        try:
            self.backend.close()
        except AttributeError:
            pass

    def get_ready_results(self):
        res = []
        while True:
            try:
                action, result = self.result_queue.get_nowait()
            except Empty:
                break
            else:
                assert action in ('network_result', 'task')
                res.append((action, result))
        return res

    def add_task(self, task):
        self.input_queue.put(task)
