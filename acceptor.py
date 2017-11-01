#! /usr/bin/env python

import message

import logging

class Acceptor(message.MessageListener):
    def __init__(self, config, network):
        message.MessageListener.__init__(self,  
            name    = 'AcceptorListenser',
            mapping = { 
            message.MSG_PROPOSAL_REQ    : self.on_proposal_request,
            message.MSG_ACCEPT_REQ      : self.on_accept_request
        })  

        self.network        = network
        self.config         = config
        self.promised_id    = 0 
        self.accepted_id    = 0 
        self.accepted_values= []

    def on_proposal_request(self, pkg, msg):
        logging.debug('process proposal request')   
        return False    
    
    def on_accept_request(self, pkg, msg):
        logging.debug('process accept request') 
        return False
