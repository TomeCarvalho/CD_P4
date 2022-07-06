# coding: utf-8

import socket
import selectors
import signal
import logging
import argparse
import time
import re

# configure logger output format
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('Load Balancer')


# used to stop the infinity loop
done = False

sel = selectors.DefaultSelector()

policy = None
mapper = None

# implements a graceful shutdown
def graceful_shutdown(signalNumber, frame):  
    logger.debug('Graceful Shutdown...')
    global done
    done = True


# n to 1 policy
class N2One:
    def __init__(self, servers):
        self.servers = servers  

    def select_server(self):
        return self.servers[0]

    def update(self, *arg):
        pass


# round robin policy: the requests are routed to all servers in sequence
class RoundRobin:
    def __init__(self, servers):
        self.servers = servers
        self.counter = 0

    def select_server(self):
        self.counter += 1
        
        if self.counter >= len(self.servers):
            self.counter = 0

        return self.servers[self.counter]
    
    def update(self, *arg):
        pass


# least connections policy: the request is routed to the server with fewer processed connections
class LeastConnections:
    def __init__(self, servers):
        self.servers = servers
        self.n_conns = len(servers) * [0] # number of connections per server, start at 0
        self.last_sel_server_idx = None
        self.sockets = [[] for _ in range(len(self.servers))]

    def select_server(self):
        idx = self.n_conns.index(min(self.n_conns)) # find index of server with least connections
        self.last_sel_server_idx = idx       # set it as the index of the last selected server
        return self.servers[idx]             # return the server

    def update(self, *arg): # arg: "+" or "-"
        op = arg[0]
        sckt = arg[1]
        #print("update op:", op)
        if op == "+":
            self.n_conns[self.last_sel_server_idx] += 1
            self.sockets[self.last_sel_server_idx].append(sckt)
        elif op == "-":
            idx = self.n_conns.index(max(self.n_conns)) # find index of server with least connections
            self.last_sel_server_idx = idx  
            is_over = False
            for i in range(len(self.sockets)):
                server = self.sockets[i]
                #print("server:",server)
                for j in range(len(server)):
                    socket = server[j]
                    if socket == sckt:
                        self.n_conns[i] -= 1
                        server.pop(j)
                        #print("i:",i)
                        #print("sckt:",sckt)
                        #print("socket:",socket)
                        is_over = True
                        break
                if is_over:
                    break
        else:
            pass
            #print("LeastConnections.update - unexpected arg:", op)
        
        #print(self.n_conns)


# least response time: the request is routed to the server with less average execution time
class LeastResponseTime:
    def __init__(self, servers):
        self.servers = servers
        self.sum_times = [0] * len(self.servers)
        self.n_elements = [0] * len(self.servers)
        #self.sockets = []
        self.sockets = [[] for _ in range(len(self.servers))]
        #for i in range len(self.servers):
        #    sockets.add([])
        # self.counter = 0

    def select_server(self):
        # if self.counter < len(self.servers):
        #     self.counter += 1
        #     return self.servers[self.counter-1]
        # agr todos já são != 0/0
        avg = len(self.sum_times) * [None]
        for i in range(len(self.servers)):
            if self.n_elements[i] == 0:
                self.last_sel_server_idx = i
                return self.servers[i]
            avg[i] = self.sum_times[i] / self.n_elements[i]
        #print("avg:", avg)
        self.last_sel_server_idx = avg.index(min(avg))
        return self.servers[self.last_sel_server_idx]

    def update(self, *arg):
        op = arg[0]
        sckt = arg[1]
        #print("update op:", op)
        if op == "+":
            self.n_elements[self.last_sel_server_idx] += 1
            self.sockets[self.last_sel_server_idx].append(sckt)
            #print("sckt add:", sckt)
            #print("idx:", self.last_sel_server_idx)
            #print("actual servers: ", self.sockets)
            #print("choosen:", self.sockets[self.last_sel_server_idx])
        elif op == "-":
            time_diff = arg[2]
            is_over = False
            for i in range(len(self.sockets)):
                server = self.sockets[i]
                #print("server:",server)
                for j in range(len(server)):
                    socket = server[j]
                    if socket == sckt:
                        self.sum_times[i] += time_diff
                        server.pop(j)
                        #print("i:",i)
                        #print("sckt:",sckt)
                        #print("socket:",socket)
                        is_over = True
                        break
                if is_over:
                    break
            #self.sum_times[self.last_sel_server_idx] += time_diff
        else:
            pass
            #print("LeastResponseTime.update - unexpected arg:", op)
        #print("n_conns:", self.n_elements)
        #print("sum_times:", self.sum_times)


POLICIES = {
    "N2One": N2One,
    "RoundRobin": RoundRobin,
    "LeastConnections": LeastConnections,
    "LeastResponseTime": LeastResponseTime
}

class SocketMapper:
    def __init__(self, policy):
        self.policy = policy
        self.map = {}
        self.cache = []

    def add(self, client_sock, upstream_server):
        if type(self.policy) == LeastConnections:
            policy.update("+",client_sock)
        if type(self.policy) == LeastResponseTime:
            self.time_start = time.time()
            policy.update("+", client_sock)

        client_sock.setblocking(False)
        sel.register(client_sock, selectors.EVENT_READ, read_client)
        upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        upstream_sock.connect(upstream_server)
        upstream_sock.setblocking(False)
        sel.register(upstream_sock, selectors.EVENT_READ, read_server)
        logger.debug("Proxying to %s %s", *upstream_server)
        self.map[client_sock] = upstream_sock

    def delete(self, sock):
        #print("sus delete")
        if sock in self.map:
            self.map.pop(sock)
            # sel.unregister(sock)
            # sock.close()
            if type(self.policy) == LeastConnections:
                policy.update("-",sock)
            elif type(self.policy) == LeastResponseTime:
                time_diff = time.time() - self.time_start
                policy.update("-",sock,time_diff)
        sel.unregister(sock)
        sock.close()
        # except KeyError:
        #     # print("SocketMapper.delete KeyError")
        #     pass

    def get_sock(self, sock):
        for client, upstream in self.map.items():
            if upstream == sock:
                return client
            if client == sock:
                return upstream
        return None
    
    def get_upstream_sock(self, sock):
        return self.map.get(sock)

    def get_all_socks(self):
        """ Flatten all sockets into a list"""
        return list(sum(self.map.items(), ()))

def accept(sock, mask):
    client, addr = sock.accept()
    logger.debug("Accepted connection %s %s", *addr)
    pol = mapper.policy
    mapper.add(client, policy.select_server())

def read(conn,mask):
    data = conn.recv(4096)
    #print("data:", data)
    if len(data) == 0: # No messages in socket, we can close down the socket
        mapper.delete(conn)
    else:
        mapper.get_sock(conn).send(data)

def read_client(conn,mask):
    #print("read client")
    data = conn.recv(4096)
    #print("data:", data)
    if len(data) == 0: # No messages in socket, we can close down the socket
        mapper.delete(conn)
    else:
        precision = re.search(b'GET /(.*) ', data).group(1)
        #print("precision:", precision)
        for prec, html in mapper.cache:
            if prec == precision:
                #print("found in cache")
                conn.send(html)
                return
        mapper.get_sock(conn).send(data)

def read_server(conn,mask):
    #print("read server")
    data = conn.recv(4096)
    #print("data:", data)
    if len(data) == 0: # No messages in socket, we can close down the socket
        mapper.delete(conn)
    else:
        precision_search = re.search(b'precision (.*) ', data)
        if precision_search is not None:
            precision = precision_search.group(1)
            #print("precision:", precision)
            mapper.cache.append((precision, data))
            #print("cache: added precision", precision)
            if len(mapper.cache) > 5:
                mapper.cache.pop(0)
                #print("cache: removed precision", precision)
        mapper.get_sock(conn).send(data)


def main(addr, servers, policy_class):
    global policy
    global mapper

    # register handler for interruption 
    # it stops the infinite loop gracefully
    signal.signal(signal.SIGINT, graceful_shutdown)

    policy = policy_class(servers)
    mapper = SocketMapper(policy)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(addr)
    sock.listen()
    sock.setblocking(False)

    sel.register(sock, selectors.EVENT_READ, accept)

    try:
        logger.debug("Listening on %s %s", *addr)
        while not done:
            events = sel.select(timeout=1)
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
                
    except Exception as err:
        logger.error(err)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pi HTTP server')
    parser.add_argument('-a', dest='policy', choices=POLICIES)
    parser.add_argument('-p', dest='port', type=int, help='load balancer port', default=8080)
    parser.add_argument('-s', dest='servers', nargs='+', type=int, help='list of servers ports')
    args = parser.parse_args()
    
    servers = [('localhost', p) for p in args.servers]
    
    main(('127.0.0.1', args.port), servers, POLICIES[args.policy])
