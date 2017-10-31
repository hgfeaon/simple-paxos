#! /usr/bin/env python

class ClusterConfig:
	addrs = {
			0 : ('127.0.0.1', 20000),
			1 : ('127.0.0.1', 20001),
			2 : ('127.0.0.1', 20002),
			3 : ('127.0.0.1', 20003),
			4 : ('127.0.0.1', 20004),
			5 : ('127.0.0.1', 20005),
			6 : ('127.0.0.1', 20006),
			7 : ('127.0.0.1', 20007),
			8 : ('127.0.0.1', 20008),
			9 : ('127.0.0.1', 20009),
		}

	def __init__(self):
		self.nodeid = 0
		pass
	def getaddr(self, nodeid):
		return ClusterConfig.addrs[nodeid]
	def getnodeid(self):
		return self.nodeid

