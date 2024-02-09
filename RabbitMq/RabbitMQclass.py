import pika, sys, os
import logging

logging.basicConfig(level = logging.DEBUG)

logging.info("A Python Rabbitmq Connector")
class RabbitmqConnector: 
	def __init__(self, host, port):
		self.host = host
		self.port = port

	def sendMessage(self, sendMessageData):
		logging.info("Sending Message")	
		isMessageSent = False
		responseMessage = ""
		try:
			if (sendMessageData.get("queue") != ""):
				if (sendMessageData.get("routing_key") != ""):
					if (sendMessageData.get("body") != ""):
						connection = pika.BlockingConnection(pika.ConnectionParameters(host = self.host, port = self.port))
						channel = connection.channel()
						channel.queue_declare(queue = sendMessageData.get("queue"))
						channel.basic_publish(exchange = sendMessageData.get("exchange"),
						                      routing_key = sendMessageData.get("routing_key"),
						                      body = sendMessageData.get("body"))	
						connection.close()
						logging.debug("Message Sent")
						isMessageSent = True
						responseMessage = "Message Sent Successfully"
					else:
						responseMessage = "Provide valid body values to sent"	
				else:
					responseMessage = "Provide valid routing key values to sent"	
			else:
				responseMessage = "Provide valid queue values to sent"	
		except:
			logging.exception("Unable to sent the message")
		return isMessageSent, responseMessage		

	def publishMessage(self, publishMessageData):
		logging.info("Publishing Message")
		isMessagePublished = False
		responseMessage = ""
		try:
			if (publishMessageData.get("exchange") != ""):
				if (publishMessageData.get("exchange_type") != ""):
					if (publishMessageData.get("body") != ""):
						connection = pika.BlockingConnection(pika.ConnectionParameters(host = self.host, port = self.port))
						channel = connection.channel()
						channel.exchange_declare(exchange = publishMessageData.get("exchange"), exchange_type = publishMessageData.get("exchange_type"))
						message = ' '.join(sys.argv[1:]) 
						channel.basic_publish(exchange = publishMessageData.get("exchange"), routing_key = publishMessageData.get("routing_key"), body = publishMessageData.get("body"))
						connection.close()
						logging.debug("Message Published")
						isMessagePublished = True
						responseMessage = "Message Published Successfully"
					else:
						responseMessage = "Provide valid body values to publish"
				else:
					responseMessage = "Provide valid exchange types values to publish"	
			else:
				responseMessage = "Provide valid exchange values to publish"			
		except:
			logging.exception("Unable to publish the message")
		return isMessagePublished, responseMessage		

	def receiveMessage(self, receiveMessageData):
		logging.info("Receiving Message")
		isMessageReceived = False
		responseMessage = ""
		try:
			if (receiveMessageData.get("queue") != ""):
				if (receiveMessageData.get("on_message_callback") != ""):
					if (receiveMessageData.get("auto_ack") != ""):
						connection = pika.BlockingConnection(pika.ConnectionParameters(host = self.host, port = self.port))
						channel = connection.channel()
						channel.queue_declare(queue = receiveMessageData.get("queue"))
						def callback(ch, method, properties, body, receiveMessageData):     
						    channel.basic_consume(queue = receiveMessageData.get("queue"), on_message_callback = receiveMessageData.get("on_message_callback"), auto_ack = receiveMessageData.get("auto_ack"))
						    channel.start_consuming()    
						logging.debug("Message Received")
						isMessageReceived = True
						responseMessage = "Message Received Successfully"
					else:
						responseMessage = "Provide valid auto ack values to receive"
				else:
					responseMessage = "Provide valid on message callback values to receive"
			else:
				responseMessage = "Provide valid queue values to receive"				
		except:
			logging.exception("Unable to receive the message")	
		return isMessageReceived, responseMessage	    
				