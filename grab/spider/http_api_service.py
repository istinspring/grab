import logging
import os
import threading
import json

from six.moves.SimpleHTTPServer import SimpleHTTPRequestHandler
from six.moves.socketserver import TCPServer
from weblib.encoding import make_str

from grab.spider.base_service import BaseService

# pylint: disable=invalid-name
logger = logging.getLogger('grab.spider.http_api_service')
# pylint: enable=invalid-name
BASE_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


class ApiHandler(SimpleHTTPRequestHandler):
    def do_GET(self): # pylint: disable=invalid-name
        if self.path == '/':
            self.home()
        elif self.path == '/api/info':
            self.api_info()
        elif self.path == '/api/stop':
            self.api_stop()
        else:
            self.not_found()

    def response(self, code=200, content=b'',
                 content_type='text/html; charset=utf-8'):
        self.send_response(code)
        self.send_header('Content-type', content_type)
        self.end_headers()
        self.wfile.write(content)

    def not_found(self):
        self.response(404)

    def api_info(self):
        info = {
            'counters': self.spider.stat.counters,
            'collections': dict((x, len(y)) for (x, y)
                                in self.spider.stat.collections.items()),
            'thread_number': self.spider.thread_number,
            'parser_pool_size': self.spider.parser_pool_size,
        }
        content = make_str(json.dumps(info))
        self.response(content=content)

    def api_stop(self):
        self.response()
        self.spider.stop()

    def home(self):
        html_file = os.path.join(BASE_DIR, 'spider/static/http_api.html')
        content = open(html_file, 'rb').read()
        self.response(content=content)


class ReuseTCPServer(TCPServer):
    allow_reuse_address = True


class HttpApiService(BaseService):
    def __init__(self, spider):
        self.spider = spider
        self.worker = self.create_worker(self.worker_callback)
        self.register_workers(self.worker)

    def pause(self):
        return

    def resume(self):
        return

    def worker_callback(self, worker):
        ApiHandler.spider = self.spider
        server = ReuseTCPServer(("", self.spider.http_api_port),
                                     ApiHandler)
        logging.debug('Serving HTTP API on localhost:%d',
                      self.spider.http_api_port)
        server.serve_forever()
