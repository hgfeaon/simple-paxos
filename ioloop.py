#! /usr/bin/env python

import socket
import select 
import logging

logging.basicConfig( format='%(asctime)-15s %(message)s', level=logging.DEBUG )

class IOHandler:
	CONTINUE		  = 0
	RECEIVE_DONE	= 1
	SEND_DONE 		= 10
	CLOSE			    = 20
	def __init__(self):
		pass
	def error(self, ioloop, conn):
		fd = conn.sock.fileno()
		logging.debug('hang up event fd: %d addr: %s', fd, str(conn.sock.getpeername()))
		ioloop.epoll.unregister( fd )
		ioloop.fd2conn[fd].close()
		del ioloop.fd2conn[fd]

	def ingress(self, ioloop, conn):
		if not conn.ingress_packet:
			conn.ingress_packet = Packet()
		phase = IOHandler.CLOSE

		try:
			phase = self.receive( conn.sock, conn.ingress_packet )
		except:
			logging.debug('packet invalid')

		if phase == IOHandler.CLOSE:
			ioloop.epoll.unregister( conn.sock.fileno() )
			del ioloop.fd2conn[ conn.sock.fileno() ]
			logging.debug('connection closed')
		elif phase == IOHandler.RECEIVE_DONE:
			pkg = conn.ingress_packet
			logging.debug('packet received. magic: %s datalen:%d data:%s', pkg.magic, pkg.length, pkg.data)
			pkg.reset()

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
				return IOHandler.RECEIVE_DONE
		# CONNECTION CLOSED
		if previous_len == packet.work_len:
			return IOHandler.CLOSE
		return IOHandler.CONTINUE

	def readbytes(self, sock, packet, n):
		readlen = 0
		while packet.work_len < n:
			data = ''
			try:
				data = sock.recv(n - packet.work_len)
			except socket.error as e:
				logging.debug('sock error: %s', str(e))
			packet.work_len += len(data)
			readlen			+= len(data)
			packet.data 	+= data
			if len( data ) == 0:
				break
		return readlen

	def send( self, sock, packet ):
		pass

class Packet:
	PHASE_MAGIC		= 1
	PHASE_LENGTH	= 2
	PHASE_DATA		= 3
	PHASE_DONE		= 4

	MAGIC_BYTES 	= 4
	LENGTH_BYTES 	= 4
	HEADER_BYTES 	= MAGIC_BYTES + LENGTH_BYTES

	def __init__(self):
		self.reset()

	def reset(self):
		self.phase		= Packet.PHASE_MAGIC
		self.magic		= ''
		self.length		= 0
		self.data 		= ''
		self.work_len 	= 0


class Connection:
	def __init__(self, sock):
		self.ingress_packet	= None
		self.egress_packet	= None
		self.sock 			= sock

	def close(self):
		if self.sock:
			self.sock.close()

class IOLoop:
	def __init__(self):
		self.sock 		= None
		self.epoll		= None
		self.fd2conn 	= {}
		self.iohandler 	= IOHandler()
		pass

	def start(self, addr = "0.0.0.0", port = 20170, backlog = 10 ):
		self.svr_sock = sock = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
		sock.setsockopt( socket.SOL_SOCKET, socket.SO_REUSEADDR, 1 )
		svr_addr = (addr, port)
		sock.bind( svr_addr )
		sock.listen( backlog )
		sock.setblocking( False )
		logging.debug("server listen @ %s", str(svr_addr))
		
		self.epoll = select.epoll()
		self.epoll.register( sock.fileno(), select.EPOLLIN ) 
		
		self.fd2conn[sock.fileno()] = Connection(sock)

		self.loop()

	def loop(self):
		timeout = 10
		epoll = self.epoll
		while True:
			events = epoll.poll( timeout )
			if not events:
				continue
			for fd, event in events:
				conn	= self.fd2conn[fd]
				socket 	= conn.sock
				if socket == self.svr_sock:
					cli_sock, addr = socket.accept()
					logging.debug('connect from %s', str(addr))
					
					cli_sock.setblocking( False )
					
					epoll.register( cli_sock.fileno(), select.EPOLLIN )
					
					self.fd2conn[ cli_sock.fileno() ] = Connection(cli_sock)

				elif event & select.EPOLLHUP:
					self.iohandler.error(self, conn)
				elif event & select.EPOLLIN:
					self.iohandler.ingress(self, conn)

		epoll.unregister(self.svr_sock)
		epoll.close()
		self.svr_sock.close()

if __name__ == '__main__':
	loop = IOLoop()
	loop.start()
