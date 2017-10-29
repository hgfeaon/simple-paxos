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
				conn.outbound_packets.append( Packet( msg.data ) )
				logging.debug('move outbound queue to conn.outbound_packets. nodeid:%d msg:%s', msg.nodeid, msg.data)
		except Queue.Empty as e:
			logging.debug('outbound queue empty now. %s', str(e))

	def on_event(self, ioloop, events):
		if not events and ioloop.nodeid > 0:
			logging.debug('event handler. try to connect nodeid:%d', 0)
			ioloop.send_message(0, 'hello world! from node:%d'%(ioloop.nodeid))
			self.sent=True
		else:
			pass
	
	def next_timeout(self):
		return 0.001

class ErrorHandler:
	def __init__(self):
		pass
	def on_error(self, ioloop, conn):
		logging.debug('hang up event fd:%d', conn.fd)
		ioloop.close_connection( conn )

class ProtocolHandler:
	DONE 		= 0
	CONTINUE 	= 1
	PAUSE		= 2
	CLOSE		= 3
	def __init__(self):
		pass
	def on_read(self, buffer, packet):
		data, start = buffer

		phase, ret = packet.parse(data, start)
		if phase == Packet.PHASE_ERROR:
			# packet invalid
			logging.debug('invalid packet:[%s]', packet.header)
			return (ProtocolHandler.CLOSE, ret)
		elif phase == Packet.PHASE_DONE:
			# full packet
			logging.debug('full packet:[%s]', packet.tostring())
			return (ProtocolHandler.CONTINUE, ret)
		else:
			# half packet
			logging.debug('half packet:[%s]', packet.tostring())
			return (ProtocolHandler.PAUSE, ret)

	def on_write(self, packets, data):
		writeout = 0
		while len( packets ) > 0:
			# TODO: buffer capacity check
			packet 	= packets.popleft()
			part	= packet.tostring()
			data.extend( part )
			writeout += len( part )
		return (ProtocolHandler.DONE, writeout)

class TransportHandler:
	IO_CONTINUE = 0
	IO_DONE		= 1
	IO_CLOSE	= 2
	def __init__(self, protocol):
		self.protocol = protocol
		pass
	def accept( self, ioloop, conn ):
		cli_sock, addr = conn.sock.accept()
		logging.debug( 'connect from %s fd:%d', str(addr), cli_sock.fileno() )
		ioloop.add_connection( Connection( cli_sock, addr ) )


	def egress( self, ioloop, conn ):
		if conn.status == Connection.CONNECTING:
			logging.debug('connect established. %s', str(conn))
			conn.status = Connection.ESTABLISHED
		if not conn.writable():
			ioloop.mod_connection( conn, select.EPOLLIN )

		self.protocol.on_write( conn.outbound_packets, conn.outbound_bytes )

		phase = TransportHandler.IO_CLOSE
		try:
			# TODO: use ringbuffer
			sent = self.send( conn.sock, conn.outbound_bytes )
			conn.outbound_bytes = conn.outbound_bytes[sent:]
			if conn.writable():
				phase = TransportHandler.IO_CONTINUE
			else:
			 	phase = TransportHandler.IO_DONE
		except IOError as e:
			logging.debug('bytes send error: %s', str(e))
		
		if phase == TransportHandler.IO_CLOSE:
			ioloop.close_connection( conn )
			logging.debug('connection closed')
		elif phase == TransportHandler.IO_DONE:
			ioloop.mod_connection( conn, select.EPOLLIN )
			logging.debug('no more bytes to send')

	def send(self, sock, data):
		sendlen		= 0
		totallen 	= len(data)

		while sendlen < totallen:
			part	=	data[sendlen:]
			try:
				sendlen += sock.send( part )
			except socket.error as e:
				logging.debug('send bytes sock error: %s bytes:%s', str(e), part)
				break
		return sendlen

	def ingress(self, ioloop, conn):
		if conn.status == Connection.CONNECTING:
			conn.status = Connection.ESTABLISHED

		phase 	= TransportHandler.IO_CLOSE
		recvlen = 0
		try:
			recvlen = self.receive( conn.sock, conn.inbound_bytes )
		except IOError as e :
			logging.debug('ingress bytes invalid : %s', str(e))

		if recvlen != 0:
			start, end = 0, len( conn.inbound_bytes )
			readin = 0
			while start < end:
				ret, readin = self.protocol.on_read( (conn.inbound_bytes, start), conn.parsing_packet )
				start += readin
				if ret == ProtocolHandler.CLOSE:
					break
				elif ret == ProtocolHandler.PAUSE:
					phase = TransportHandler.IO_CONTINUE
					break
				elif ret == ProtocolHandler.CONTINUE:
					conn.inbound_packets.append( conn.parsing_packet )
					ioloop.received_packets+=1
					logging.debug('ioloop.received_packets:%d', ioloop.received_packets)
					conn.parsing_packet = Packet()
					phase = TransportHandler.IO_CONTINUE
				else:
					logging.debug('protocol return unknown phase')
					break

			if readin > 0:
				conn.inbound_bytes = conn.inbound_bytes[readin:]	

		if phase == TransportHandler.IO_CLOSE:
			ioloop.close_connection( conn )

	def receive( self, sock, data ):
		readlen = 0
		STEP = 1024
		while True:
			part = ''
			try:
				part = sock.recv( STEP )
			except socket.error as e:
				logging.debug('fd %d read sock error: %s', sock.fileno(), str(e))
			readlen	+= len( part )
			data.extend( part )
			if len( part ) < STEP:
				break
		return readlen

class Packet:
	PHASE_HEADER	= 1
	PHASE_DATA		= 2
	PHASE_DONE		= 3
	PHASE_ERROR		= 4

	MAGIC_BYTES 	= 4
	LENGTH_BYTES 	= 4
	HEADER_BYTES 	= MAGIC_BYTES + LENGTH_BYTES

	def __init__(self, data = ''):
		self.reset()
		self.data 	= data
		self.header = ''
		self.length = len( data )

	def reset(self):
		self.phase		= Packet.PHASE_HEADER
		self.magic		= ''
		self.length		= 0
		self.data 		= ''
		self.work_len 	= 0

	def tostring(self):
		return 'paxo%04d%s' % ( len( self.data ), self.data )

	def parse(self, data, start = 0):
		end = len(data)
		bufflen = end - start
		readin = 0
		if self.phase == Packet.PHASE_HEADER:
			if bufflen + len( self.header ) < Packet.HEADER_BYTES:
				self.header += data[start:]
				return (self.phase, bufflen)
			else:
				readin = Packet.HEADER_BYTES - len( self.header )
				self.header += data[ start : start + readin ]
				self.magic 	= self.header[ : Packet.MAGIC_BYTES ]
				if not self.header[ Packet.MAGIC_BYTES: ].isdigit():
					return ( Packet.PHASE_ERROR, readin )
				else:
					self.length	= int( self.header[ Packet.MAGIC_BYTES : ] )
				self.phase = Packet.PHASE_DATA
				start 	+= readin
				bufflen -= readin

		if self.phase == Packet.PHASE_DATA:	
			remain = self.length - len( self.data )
			if bufflen >= remain :
				readin += remain
				self.data += data[ start : start + remain ]
				self.phase = Packet.PHASE_DONE
			else:
				self.data += data[ start : ]
				readin += bufflen
			return (self.phase, readin)
		return (Packet.PHASE_DONE, 0)

class Connection:
	CONNECTING	= 0
	ESTABLISHED = 1
	def __init__(self, sock, addr = None, blocking = False):
		self.parsing_packet		= Packet()
		self.inbound_bytes		= bytearray()
		self.outbound_bytes		= bytearray()
		self.inbound_packets	= collections.deque()
		self.outbound_packets	= collections.deque()
		self.sock 				= sock
		self.fd					= sock.fileno()
		self.status				= Connection.CONNECTING
		self.addr				= addr # peer addr
		sock.setblocking(blocking)
	def writable(self):
		return len( self.outbound_bytes ) or len( self.outbound_packets )
	def close(self):
		if self.sock:
			self.sock.close()

	def __str__(self):
		return 'fd:%d' % (self.fd)

class IOLoop:
	def __init__(self):
		self.received_packets 	= 0
		self.sent_packets		= 0
		self.next_connid 	= 0
		self.listen_conn	= None
		self.notify_poll	= None
		self.notify_send	= None
		self.notify_flag	= False
		self.nodeid			= 0
		self.epoll			= None
		self.fd2conn 		= {}
		self.addr2conn		= {}
		self.iohandler 		= TransportHandler(ProtocolHandler())
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
		# need lock
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
						self.iohandler.accept(self, conn)
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
		logging.debug('closing connection.')
		self.epoll.unregister( conn.fd )
		self.fd2conn[ conn.fd ].close()
		del self.fd2conn[ conn.fd ]
		del self.addr2conn[ conn.addr ]
			
	def add_connection(self, conn, EVENTS = select.EPOLLIN):
		conn.sock.setblocking( False )
		self.fd2conn[ conn.fd ] 	= conn
		self.addr2conn[ conn.addr ] = conn
		self.epoll.register( conn.fd, EVENTS )

	def open_connection(self, addrkey, EVENTS = select.EPOLLOUT):
		if addrkey in self.addr2conn:
			return self.addr2conn[ addrkey ]
			
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
