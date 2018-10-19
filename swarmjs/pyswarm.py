from Queue import Queue

__author__ = 'sherif'

import random
from tornado.websocket import WebSocketHandler
import tornado.ioloop
import tornado.web
import tornado.httpserver
from tornado.web import MissingArgumentError
import time
import os, uuid
import urllib
import os
import json
import csv

__author__ = 'sherif'

__JOBS__ = 'JOBS/'

SWARM = {}
SOCKETS = {}
WORKERS = {}
OWNERS = Queue()


def load_data(job_id, chunk_id):
    filename = __JOBS__ + str(job_id) + '/' + str(chunk_id) + '.csv'
    a = []
    csv_reader = csv.reader(open(filename, 'rU'), delimiter=' ', quotechar='|')
    for row in csv_reader:
        a.append(row)

    return a


def load_script(job_id):
    filename = __JOBS__ + str(job_id) + '/script.js'
    with open(filename, 'r') as f:
        data = f.read()
        return data


def chunk_data(job_id):
    data_file = __JOBS__ + job_id + '/data.csv'
    path, filename = os.path.split(data_file)
    basename, ext = os.path.splitext(filename)
    with open(data_file, 'rU') as f_in:
        try:
            index = 1
            f = open(data_file, "rU")
            totalNumline = len(f.readlines())
            if totalNumline<=10:
                lines=10
            else:
                lines = (totalNumline / 10)
            for i, line in enumerate(f_in):

                if i % lines == 0:
                    f_output = os.path.join(path, '{}{}{}'.format("", index, ext))
                    f_out = open(f_output, 'w')
                    index += 1
                f_out.write(line)

        finally:
            f_out.close()
        return index - 1


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("web/html/index.html")


class JobCreatorHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("web/html/jcreator.html")


class JobWorkerHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("web/html/workshell.html")


class JobUploaderHandler(tornado.web.RequestHandler):
    def post(self):
        fileinfo1 = self.request.files['filearg1'][0]
        fileinfo2 = self.request.files['filearg2'][0]
        job_ID = str(uuid.uuid4())
        job_dir = __JOBS__ + job_ID + '/'
        os.makedirs(job_dir)
        fh = open(job_dir + fileinfo1['filename'], 'w')
        fh.write(fileinfo1['body'])
        fh.close()
        fh = open(job_dir + fileinfo2['filename'], 'w')
        fh.write(fileinfo2['body'])
        fh.close()
        print "Your Data file is", fileinfo1['filename']
        print "Your Script file is", fileinfo2['filename']
        newurl = "/job/creator/joblog?jobid=" + job_ID
        self.redirect(newurl)


class JobLogHandler(tornado.web.RequestHandler):
    def get(self):
        try:
            job_id = self.get_argument('jobid')
            print "this is the job id", job_id
            self.render("web/html/jobshell.html", jobref=job_id)
        except MissingArgumentError:
            self.render("web/html/index.html")


class WebSocketHandler(tornado.websocket.WebSocketHandler):

    CURRENT_JOB = None

    def open(self):
        swarm_id = str(uuid.uuid4())
        SWARM[swarm_id] = {'socket': self, 'role': 'unknown'}
        SOCKETS[self] = swarm_id
        msg = {"type": "connection_ready", "swarm_id": swarm_id}
        self.write_message(json.dumps(msg), binary=False)
        print 'new swarm connected', swarm_id, 'total connected clients:', len(SWARM), "sockets", len(SOCKETS)

    def on_message(self, message):
        print message
        self.parse_message(message)
        # self.write_message("nothing")

    def on_close(self):
        if SOCKETS[self] in WORKERS:
            WORKERS.pop(SOCKETS[self])
            if WebSocketHandler.CURRENT_JOB['sub_jobs'] > 0:
                WebSocketHandler.CURRENT_JOB['sub_jobs'] -= 1

        if SOCKETS[self] in SWARM:
            SWARM.pop(SOCKETS[self])

        SOCKETS.pop(self)
        print 'new swarm after disconnect total connected clients:', len(SWARM), "sockets", len(SOCKETS)
        print("WebSocket closed")

    def parse_message(self, msg):
        msg_dic = json.loads(msg)
        if msg_dic['type'] == 'client_role':

            SWARM[SOCKETS[self]]['role'] = msg_dic['role']

            if msg_dic['role'] == 'job_owner':
                owner = SWARM.pop(SOCKETS[self])
                owner['job_id'] = msg_dic['job_id']
                sub_jobs = chunk_data(owner['job_id'])
                owner['sub_jobs'] = sub_jobs
                owner['todo_jobs'] = sub_jobs
                owner['job_result'] = []

                if WebSocketHandler.CURRENT_JOB is None:
                    WebSocketHandler.CURRENT_JOB = owner
                else:
                    OWNERS.put(owner)
            else:
                worker = SWARM.pop(SOCKETS[self])
                worker['status'] = 'free'
                WORKERS[SOCKETS[self]] = worker

        if msg_dic['type'] == 'work_result':
            WORKERS[SOCKETS[self]]['status'] = 'free'
            if WebSocketHandler.CURRENT_JOB['sub_jobs'] > 0:
                WebSocketHandler.CURRENT_JOB['sub_jobs'] -= 1
                print "remain jobs", WebSocketHandler.CURRENT_JOB['sub_jobs']
                WebSocketHandler.CURRENT_JOB['job_result'].append(msg_dic['data'])

        self.dispatch_jobs()

    def dispatch_jobs(self):
        if WebSocketHandler.CURRENT_JOB is not None and WebSocketHandler.CURRENT_JOB['sub_jobs'] == 0:
            msg = {'type': 'job_result', 'result': WebSocketHandler.CURRENT_JOB['job_result']}

            if WebSocketHandler.CURRENT_JOB['socket'] in SOCKETS:
                WebSocketHandler.CURRENT_JOB['socket'].write_message(json.dumps(msg), binary=False)
                script = load_script(WebSocketHandler.CURRENT_JOB['job_id'])
                msg = {'type': 'rCode', 'data': script}
                WebSocketHandler.CURRENT_JOB['socket'].write_message(json.dumps(msg), binary=False)

            while not OWNERS.empty():
                WebSocketHandler.CURRENT_JOB = OWNERS.get()
                if WebSocketHandler.CURRENT_JOB['socket'] in SOCKETS:
                    break
                else:
                    WebSocketHandler.CURRENT_JOB = None
        print "Current JOB =>>>", WebSocketHandler.CURRENT_JOB
        if WebSocketHandler.CURRENT_JOB is not None and WebSocketHandler.CURRENT_JOB['todo_jobs'] != 0:
            for key in WORKERS:
                if WORKERS[key]['status'] == 'free':
                    print "iam here........"
                    # TODO: load data and script send data and script
                    data = load_data(WebSocketHandler.CURRENT_JOB['job_id'], WebSocketHandler.CURRENT_JOB['todo_jobs'])
                    msg = {'type': 'Data', 'data': data}
                    WORKERS[key]['socket'].write_message(json.dumps(msg), binary=False)
                    script = load_script(WebSocketHandler.CURRENT_JOB['job_id'])
                    msg = {'type': 'Code', 'data': script}
                    WORKERS[key]['socket'].write_message(json.dumps(msg), binary=False)

                    WORKERS[key]['status'] = 'busy'
                    WebSocketHandler.CURRENT_JOB['todo_jobs'] -= 1
                    if WebSocketHandler.CURRENT_JOB['todo_jobs'] == 0:
                        break


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", MainHandler),
            (r"/job/creator", JobCreatorHandler),
            (r"/job/creator/upload", JobUploaderHandler),
            (r"/job/creator/joblog", JobLogHandler),
            (r"/job/worker/log", JobWorkerHandler),
            (r"/ws", WebSocketHandler),
        ]

        settings = {
            "debug": True,
            "static_path": os.path.join(os.path.dirname(__file__), "web")
        }
        print settings["static_path"]
        tornado.web.Application.__init__(self, handlers, **settings)


if __name__ == "__main__":
    app = Application()
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
