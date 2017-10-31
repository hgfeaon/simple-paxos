#! /usr/bin/python

import ioloop
import clusterconfig
import threading
import Queue

class IOThread(threading.Thread):
	def __init__(self, config, ioloop):
		threading.Thread.__init__(self)
		self.config = config
		self.ioloop = ioloop

	def run(self):
		self.ioloop.start(svr_addr = self.config.getaddr(self.config.getnodeid()))

class Network:
	def __init__(self, config):
		self.inbound	= Queue.queue()
		self.outbound	= Queue.queue()
		self.ioloop		= ioloop.IOLoop(inbound_que = self.inbound, outbound_que = self.outbound)
		self.iothread 	= IOThread(config, self.ioloop)
		self.config 	= config
		self.listeners 	= {}
		pass
	def start(self):
		self.iothread.start()

	def send_msg(self, pkg):
		self.ioloop.send_message(pkg)

	def add_listener(self, type, listener):
		pass

if __name__ == '__main__':
	network = Network(clusterconfig.ClusterConfig())
	network.start()
