#! /usr/bin/env python

import socket
import select 
import collections
import logging
import sys
import Queue

logging.basicConfig( format='%(asctime)-15s %(message)s', level=logging.DEBUG )

class ClusterConfig:
	addrs = {
			0 : ('127.0.0.1', 20000),
			1 : ('127.0.0.1', 20001),
			2 : ('127.0.0.1', 20002),
			3 : ('127.0.0.1', 20003),
			4 : ('127.0.0.1', 20004),
			5 : ('127.0.0.1', 20005),
			6 : ('127.0.0.1', 20006),
		}

	def __init__(self):
		pass
	def getaddr(self, nodeid):
		return ClusterConfig.addrs[nodeid]

clusterconfig = ClusterConfig()

class Message:
	def __init__(self, nodeid, data = ''):
		self.nodeid 	= nodeid
		self.data 		= data
		self.type		= 0

class EventHandler:
	def __init__(self):
		self.sent = False
		pass
	def on_notify(self, ioloop):
		msg = ioloop.notify_poll.sock.recv(1)
		logging.debug('on_notify received dummy message:%s', msg)
		ioloop.notify_flag = False
		try:
			while True:
				msg 	= ioloop.outbound_queue.get_nowait()
				conn 	= ioloop.open_connection( clusterconfig.getaddr(msg.nodeid) )
				if len(conn.outbound_packets) == 0:
					ioloop.mod_connection(conn, select.EPOLLOUT)
				conn.outbound_packets.append( Packet('paxo%04d%s' % ( len(msg.data), msg.data) ) )
				logging.debug('move outbound queue to conn.outbound_packets. nodeid:%d msg:%s', msg.nodeid, msg.data)
		except Queue.Empty as e:
			logging.debug('outbound queue empty now. %s', str(e))

	def on_event(self, ioloop, events):
		if not events and ioloop.nodeid == 0:
			logging.debug('event handler. try to connect nodeid:%d', 1)
			ioloop.send_message(1, 'hello world! from node:%d'%(ioloop.nodeid))
			self.sent=True
		else:
			pass
	
	def next_timeout(self):
		return 5

class AcceptHandler:
	def __init(self):
		pass
	def accept(self, ioloop, conn):	
		cli_sock, addr = conn.sock.accept()
		logging.debug('connect from %s fd:%d', str(addr), cli_sock.fileno())
		ioloop.add_connection( Connection(cli_sock, addr) )

class ByteHandler:
	def __init__(self):
		pass
	def egress(self, ioloop, conn):
		pass
	def ingress(self, ioloop, conn):
		pass

class IOHandler:
	IO_CONTINUE	= 0
	IO_DONE		= 1
	IO_CLOSE	= 2
	def __init__(self):
		pass
	def error(self, ioloop, conn):
		logging.debug('hang up event fd:%d', conn.fd)
		ioloop.close_connection( conn )

	def egress(self, ioloop, conn):
		if conn.status == Connection.CONNECTING:
			logging.debug('connect established. %s', str(conn))
			conn.status = Connection.ESTABLISHED
		if not conn.egress_packet:
			if len( conn.outbound_packets ) == 0:
				ioloop.mod_connection( conn, select.EPOLLIN )
			else:
				conn.egress_packet = conn.outbound_packets.popleft()
		phase = IOHandler.IO_CLOSE
		try:
			phase = self.send( conn.sock, conn.egress_packet )
		except IOError as e:
			logging.debug('packet send error: %s', str(e))
		
		if phase == IOHandler.IO_CLOSE:
			ioloop.close_connection( conn )
			logging.debug('connection closed')
		elif phase == IOHandler.IO_DONE:
			conn.egress_packet = None
			if len( conn.outbound_packets ) == 0:
				ioloop.mod_connection( conn, select.EPOLLIN )
				logging.debug('no more packets to send')

	def send(self, sock, packet):
		while packet.work_len < packet.length:
			self.sendbytes(sock, packet, packet.length)

		if packet.work_len >= packet.length:
			logging.debug('packet sent. len:%d data:%s', packet.work_len, packet.data)
			return IOHandler.IO_DONE
		return IOHandler.IO_CONTINUE

	def sendbytes(self, sock, packet, n):
		writelen = 0
		while packet.work_len < n:
			data = packet.data[packet.work_len:]
			ret = 0
			try:
				ret = sock.send(data)
			except socket.error as e:
				logging.debug('send sock error: %s', str(e))
			packet.work_len += len(data)
			writelen		+= len(data)
			if len( data ) == 0:
				break
		return writelen

	def ingress(self, ioloop, conn):
		if conn.status == Connection.CONNECTING:
			conn.status = Connection.ESTABLISHED
		if not conn.ingress_packet:
			conn.ingress_packet = Packet()
		phase = IOHandler.IO_CLOSE

		try:
			phase = self.receive( conn.sock, conn.ingress_packet )
		except IOError as e :
			logging.debug('packet invalid : %s', str(e))

		if phase == IOHandler.IO_CLOSE:
			ioloop.close_connection( conn )
			logging.debug('connection closed')
		elif phase == IOHandler.IO_DONE:
			pkg = conn.ingress_packet
			logging.debug('packet received. magic:%s len:%d data:%s', pkg.magic, pkg.length, pkg.data)
			conn.inbound_packets.append(pkg)
			conn.ingress_packet = None
	
	def receive( self, sock, packet ):
		previous_len = packet.work_len

		if packet.phase == Packet.PHASE_MAGIC:
			# READ MAGIC FIELD
			self.readbytes( sock, packet, Packet.MAGIC_BYTES )
			if packet.work_len == Packet.MAGIC_BYTES:
				packet.phase = Packet.PHASE_LENGTH
				packet.magic = packet.data
				packet.data  = ''

		if packet.phase == Packet.PHASE_LENGTH:
			# READ LENGTH FIELD
			self.readbytes( sock, packet, Packet.HEADER_BYTES )
			if packet.work_len == Packet.HEADER_BYTES:
				packet.phase = Packet.PHASE_DATA
				packet.length= int(packet.data)
				packet.data  = ''

		if packet.phase == Packet.PHASE_DATA:
			# READ DATA FIELD
			self.readbytes( sock, packet, packet.length + Packet.HEADER_BYTES )
			if packet.length == len(packet.data):
				packet.phase = Packet.PHASE_DONE
				return IOHandler.IO_DONE
		# CONNECTION CLOSED
		if previous_len == packet.work_len:
			return IOHandler.IO_CLOSE
		return IOHandler.IO_CONTINUE

	def readbytes(self, sock, packet, n):
		readlen = 0
		while packet.work_len < n:
			data = ''
			try:
				data = sock.recv(n - packet.work_len)
			except socket.error as e:
				logging.debug('fd %d read sock error: %s', sock.fileno(), str(e))
			packet.work_len += len(data)
			readlen			+= len(data)
			packet.data 	+= data
			if len( data ) == 0:
				break
		return readlen

class Packet:
	PHASE_MAGIC		= 1
	PHASE_LENGTH	= 2
	PHASE_DATA		= 3
	PHASE_DONE		= 4

	MAGIC_BYTES 	= 4
	LENGTH_BYTES 	= 4
	HEADER_BYTES 	= MAGIC_BYTES + LENGTH_BYTES

	def __init__(self, data = ''):
		self.reset()
		self.data 	= data
		self.length = len( data )

	def reset(self):
		self.phase		= Packet.PHASE_MAGIC
		self.magic		= ''
		self.length		= 0
		self.data 		= ''
		self.work_len 	= 0


class Connection:
	CONNECTING	= 0
	ESTABLISHED = 1
	def __init__(self, sock, addr = None, blocking = False):
		self.ingress_packet		= None
		self.egress_packet		= None
		self.inbound_packets	= collections.deque()
		self.outbound_packets	= collections.deque()
		self.sock 				= sock
		self.fd					= sock.fileno()
		self.status				= Connection.CONNECTING
		self.addr				= addr # peer addr
		sock.setblocking(blocking)
	def close(self):
		if self.sock:
			self.sock.close()

	def __str__(self):
		return 'fd:%d' % (self.fd)

class IOLoop:
	def __init__(self):
		self.listen_conn	= None
		self.notify_poll	= None
		self.notify_send	= None
		self.notify_flag	= False
		self.nodeid			= 0
		self.epoll			= None
		self.fd2conn 		= {}
		self.addr2fd		= {}
		self.iohandler 		= IOHandler()
		self.accepthandler 	= AcceptHandler()
		self.eventhandler	= EventHandler()
		self.inbound_queue	= Queue.Queue()
		self.outbound_queue = Queue.Queue()
		pass

	def start(self, nodeid = 0, backlog = 10 ):
		self.nodeid 	= nodeid
		self.epoll 		= select.epoll()
		sock 			= socket.socket( socket.AF_INET, socket.SOCK_STREAM )
		sock.setsockopt( socket.SOL_SOCKET, socket.SO_REUSEADDR, 1 )
		svr_addr = clusterconfig.getaddr(nodeid)
		sock.bind( svr_addr )
		sock.listen( backlog )
		(s1, s2) = socket.socketpair()
		self.notify_poll = Connection(s1)
		self.notify_send = Connection(s2)
		logging.debug("listen_fd:%d notify_poll_fd:%d notify_send_fd:%d", sock.fileno(), s1.fileno(), s2.fileno())	
		self.add_connection( self.notify_poll )	

		logging.debug("[node-%02d] server listen @ %s", nodeid, str(svr_addr))
		self.listen_conn = Connection(sock)
		self.add_connection( self.listen_conn )
		self.loop()

	def send_message(self, nodeid, data):
		self.outbound_queue.put(Message(nodeid, data))
		self.notify()

	def notify(self):
		if not self.notify_flag:
			self.notify_send.sock.send('1')
			self.notify_flag = True

	def loop(self):
		epoll = self.epoll
		while True:
			events = epoll.poll( self.eventhandler.next_timeout() )
			if events:
				for fd, event in events:
					conn	= self.fd2conn[fd]
					if self.notify_poll.fd == fd:
						self.eventhandler.on_notify(self)
						continue
					if self.listen_conn.fd == fd:
						self.accepthandler.accept(self, conn)
						continue
					if event & select.EPOLLHUP:
						self.iohandler.error(self, conn)
						continue
					if event & select.EPOLLIN:
						self.iohandler.ingress(self, conn)
					if event & select.EPOLLOUT:
						self.iohandler.egress(self, conn)
			self.eventhandler.on_event(self, events)
		self.shutdown()

	def shutdown(self):
		self.close_connection( self.listen_conn )
		self.epoll.close()

	def close_connection(self, conn):
		self.epoll.unregister( conn.fd )
		self.fd2conn[ conn.fd ].close()
		del self.fd2conn[ conn.fd ]
		if conn.addr in self.addr2fd and self.addr2fd[ conn.addr ] == conn.fd:
			del self.addr2fd[ conn.addr ]
		else:
			logging.warning('conn.addr:%s not in mapping or fd not match', str(conn.addr))
			
	def add_connection(self, conn, EVENTS = select.EPOLLIN):
		conn.sock.setblocking( False )
		self.fd2conn[ conn.fd ] = conn
		self.addr2fd[ conn.addr ] = conn.fd
		self.epoll.register( conn.fd, EVENTS )

	def open_connection(self, addrkey, EVENTS = select.EPOLLOUT):
		if addrkey in self.addr2fd and self.addr2fd[addrkey] in self.fd2conn:
			return self.fd2conn[ self.addr2fd[ addrkey ] ]
			
		sock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
		sock.setblocking( False )
		logging.debug('connect to %s', str(addrkey))
		try:
			sock.connect( addrkey )
		except socket.error, msg:
		 	logging.debug('connect exception code:%s msg:%s', msg[0], msg[1])
		conn = Connection( sock, addrkey )
		self.add_connection( conn, EVENTS )
		return conn

	def mod_connection(self, conn, EVENTS):
		self.epoll.modify(conn.fd, EVENTS)

if __name__ == '__main__':
	loop = IOLoop()
	if len( sys.argv ) <= 2:
		loop.start( nodeid = int(sys.argv[1]) )
	else:
		loop.start()
