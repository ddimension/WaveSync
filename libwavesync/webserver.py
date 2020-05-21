import threading
import time
import http.server as SimpleHTTPServer
import socketserver as SocketServer

class WebServerHandler(
        SimpleHTTPServer.SimpleHTTPRequestHandler
        ):

    def do_addremove(self):
        root, action, channel = self.path.split('/')
        if channel is None:
            return self.do_fail(b'Empty channel name.')

        address, port = channel.split(':')
        if address is None:
            return self.do_fail(b'Invalid channel address.')

        if int(port)<1:
            return self.do_fail(b'Invalid channel port.')

        if action == "add":
            if not self.server.packetizer.add_channel((address,int(port))):
                return self.do_fail(b'Failed to add channel')
        elif action == "remove":
            if not self.server.packetizer.remove_channel((address,int(port))):
                return self.do_fail(b'Failed to remove channel')
        else:
            return self.do_fail('Invalid command')

        self.send_response(200)
        self.send_header('Content-type','text/plain')
        self.end_headers()

        if action == "add":
            self.wfile.write(b"Added the channel")
            print("http api: added a new channel %s:%s" %(address,port))
        elif action == "remove":
            self.wfile.write(b"Remove the channel")
            print("http api: remove a channel %s:%s" %(address,port))

    def do_list(self):
        self.send_response(200)
        self.send_header('Content-type','text/plain')
        self.end_headers()

        for channel in self.server.packetizer.get_channels():
            tmp = "%s:%d\n" % channel
            self.wfile.write(tmp.encode('utf-8'))

        print("http api: listed channel")

    def do_fail(self, error):
        self.send_response(500)
        self.send_header('Content-type','text/plain')
        self.end_headers()
        self.wfile.write(error)
        print("http api: error message: "+error)


    def do_GET(self):
        if self.path.startswith('/add/'):
            return self.do_addremove()
        elif self.path.startswith('/remove/'):
            return self.do_addremove()
        elif self.path.startswith('/list'):
            return self.do_list()

        self.do_fail(b'Unknown request.')


class WebServer(object):
    """ WebServer class
        Run as thread
    """

    def __init__(self, packetizer, interval=1):
        """ Constructor
        :type interval: int
        :param interval: Check interval, in seconds
        """
        self.interval = interval
        self.packetizer = packetizer

        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True                            # Daemonize thread
        thread.start()                                  # Start the execution

    def run(self):
        """ Method that runs forever """
        Handler = WebServerHandler
        httpd = SocketServer.TCPServer(("", 8099), WebServerHandler)
        httpd.packetizer = self.packetizer
        httpd.serve_forever()

