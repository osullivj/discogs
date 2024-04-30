# Mininal Tornado Web Server JOS 2016-05-04
# Requirements: Python 3.4, Tornado 4.3
# Launch at the command line with...
# c:\python34\python mtwserver.py
import functools
import json
import logging
import os
import os.path
import socket
import urllib.parse
import tornado.ioloop
import tornado.web
import tornado.httpclient
from tornado.simple_httpclient import HTTPTimeoutError
from tornado.httpclient import HTTPClientError

import config

# Some constants - hack these to change the port, or the home page
PORT=9090
HOST=socket.gethostname( )
HomePage="<html><body><p>HomePage http://%s:%d</p></body></html>" % ( HOST, PORT)
TestPage="<html><body><p>TestPage http://%s%s:%d</p></body></html>"


class Discogs(object):
    def __init__(self):
        self.io_loop = None
        self.http_client = tornado.httpclient.AsyncHTTPClient()
        self.cfg = config.DISCOGS
        self.results = {}

    def get_cache_dict(self, cache_path):
        # walk from root to leaf node in result cache
        cache_dict = self.results
        for elem in cache_path:
            cache_dict = cache_dict.setdefault(elem, dict())
        return cache_dict

    async def on_started(self, io_loop):
        self.io_loop = io_loop
        # compose and dispatch init query
        query_fmt = self.cfg['init_query']
        query_url = query_fmt % self.cfg
        await self.load_query_results(query_url)

    def load_sub_cache(self, result_file_path):
        try:
            if os.path.exists(result_file_path):
                with open(result_file_path, 'rt') as result_file:
                    json_result_set = result_file.read()
                    result_set = json.loads(json_result_set)
                    return result_set
        except json.JSONDecodeError as ex:
            logging.error(f'load_result_set: JSON {result_file_path}\n{ex}')
        return None


    def save_sub_cache(self, cache_path):
        persist_file_base = '_'.join(cache_path)
        persist_file_path = os.path.join(self.cfg['root_dir'], 'dat', f'{persist_file_base}.json')
        # open the file in write-only more so we overwrite contents
        with open(persist_file_path, 'w') as persist_file:
            cache_dict = self.get_cache_dict(cache_path)
            json.dump(cache_dict, persist_file)


    def add_sub_cache(self, cache_path, sub_cache):
        # the key within the payload should match the last element of result_cache_path
        logging.info(f'add_sub_cache: {cache_path}')
        object_count = 0
        # payload should be a list...
        if sub_cache:
            # walk from root to leaf node in result cache
            cache_dict = self.get_cache_dict(cache_path)
            # the payload will be a list of dicts, each with an id, if
            # this is a fresh query. But if loaded from file, the sub_cache
            # will be a dict of dicts
            if isinstance(sub_cache, dict):
                cache_dict.update(sub_cache)
                object_count = len(sub_cache)
            elif isinstance(sub_cache, list):
                for obj in sub_cache:
                    oid = obj.get('id')
                    if oid:
                        cache_dict[oid] = obj
                        object_count += 1
                    else:
                        logging.info(f'add_sub_cache: NOID {cache_path} {obj}')
            else:
                logging.error(f'add_sub_cache: TYPE {cache_path} {type(sub_cache)}')
        else:
            logging.error(f'add_sub_cache: EMPTY {cache_path}')
        return object_count


    def add_result(self, cache_path, result):
        # the key within the payload should match the last element of result_cache_path
        logging.info(f'add_result: {cache_path}')
        payload_key = cache_path[-1]
        payload = result.get(payload_key)
        return self.add_sub_cache( cache_path, payload)


    async def load_query_results(self, query_url):
        # figure out path for json file cache of results
        parsed_query = urllib.parse.urlparse(query_url)
        # [1:] split below to throw away the leading _
        result_file_base = parsed_query.path.replace('/', '_')[1:]
        result_file_path = os.path.join(self.cfg['root_dir'], 'dat', f'{result_file_base}.json')
        result_cache_path = result_file_base.split('_')
        logging.info(f'dispatch_query: QUERY:{query_url} {result_file_path}')
        fs_sub_cache = self.load_sub_cache(result_file_path)
        oc = 0
        if fs_sub_cache:
            oc = self.add_sub_cache(result_cache_path, fs_sub_cache)
            logging.info(f'dispatch_query: LOAD:{result_file_path} {oc}')
            return oc
        oc = await self.dispatch_query(query_url, result_cache_path)

    async def dispatch_query(self, query_url, cache_path):
        # first, the HTTP op...
        logging.info(f'dispatch_query: QUERY:{query_url} {cache_path}')
        try:
            http_response = await self.http_client.fetch(query_url)
        except Exception as ex:
            logging.error(f'dispatch_query: HTTP {query_url}\n{ex}')
            return 0
        result = json.loads(http_response.body)
        oc = self.add_result(cache_path, result)
        # is this result set paginated? If so we need to dispatch next query...
        pagination = result.get('pagination')
        if pagination:
            logging.info(f'dispatch_query: PAG:{query_url}')
            # pagination is in play, so query yielded partial result set
            next_query = pagination['urls'].get('next')
            if next_query:
                self.io_loop.add_callback(self.dispatch_query, next_query, cache_path)
            else:   # no next query, ergo query complete, so let's persist
                self.save_sub_cache(cache_path)
        else:   # not paginated, so let's save now...
            self.save_sub_cache(cache_path)
        return oc


class RootHandler( tornado.web.RequestHandler):
    def get( self):
        self.write( HomePage)


class TestHandler( tornado.web.RequestHandler):
    def get( self):
        self.write( TestPage % ( HOST, self.request.uri, PORT))


class ExitHandler( tornado.web.RequestHandler):
    def get( self):
        tornado.ioloop.IOLoop.current( ).stop( )


def make_app( ):
    return tornado.web.Application([
        (r'/', RootHandler),
        (r'/exit', ExitHandler),
		(r'/.*', TestHandler),
    ])

if __name__ == "__main__":
    # tornado logs to stdout by default - we want it in a file in the %TEMP% dir
    logf = '%s\\discogs_%d.log' % ( os.environ.get('TEMP'), os.getpid( ))
    logfmt = '%(asctime)s %(levelname)s %(thread)s %(message)s'
    logging.basicConfig( filename=logf, format=logfmt, level=logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler())
    app = make_app( )
    app.listen( PORT)
    svc = Discogs()
    io_loop = tornado.ioloop.IOLoop.current()
    io_loop.add_callback(svc.on_started, io_loop)
    io_loop.start()
