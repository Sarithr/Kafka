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
		except:
			logging.exception("Unable to sent the message")
		return isMessageSent, responseMessage		

	def publishMessage(self, publishMessageData):
		logging.info("Publishing Message")
		isMessagePublished = False
		responseMessage = ""
		try:
			connection = pika.BlockingConnection(pika.ConnectionParameters(host = self.host, port = self.port))
			channel = connection.channel()
			channel.exchange_declare(exchange = publishMessageData.get("exchange"), exchange_type = publishMessageData.get("exchange_type"))
			message = ' '.join(sys.argv[1:]) 
			channel.basic_publish(exchange = publishMessageData.get("exchange"), routing_key = publishMessageData.get("routing_key"), body = publishMessageData.get("body"))
			connection.close()
			logging.debug("Message Published")
			isMessagePublished = True
			responseMessage = "Message Published Successfully"
		except:
			logging.exception("Unable to publish the message")
		return isMessagePublished, responseMessage		

	def receiveMessage(self, receiveMessageData):
		logging.info("Receiving Message")
		isMessageReceived = False
		responseMessage = ""
		try:
			connection = pika.BlockingConnection(pika.ConnectionParameters(host = self.host, port = self.port))
			channel = connection.channel()
			channel.queue_declare(queue = receiveMessageData.get("queue"))
			def callback(ch, method, properties, body, receiveMessageData):     
			    channel.basic_consume(queue = receiveMessageData.get("queue"), on_message_callback = receiveMessageData.get("on_message_callback"), auto_ack = receiveMessageData.get("auto_ack"))
			    channel.start_consuming()    
			logging.debug("Message Received")
			isMessageReceived = True
			responseMessage = "Message Received Successfully"
		except:
			logging.exception("Unable to receive the message")	
		return isMessageReceived, responseMessage	    
				
sendMessageData = {"queue" : "hello",
				"exchange" : "",
				"routing_key" : "hello",
				"body" : "Hello World!"}

publishMessageData = {"exchange" : "logs",
					"exchange_type" : "fanout",
					"routing_key" : "",
					"body" : "Hello World!"}				
receiveMessageData = {"queue" : "hello",
					"on_message_callback" : "callback",
					"auto_ack" : "True"}

rabbitmqObject = RabbitmqConnector("localhost", 5672)
rabbitmqObject.sendMessage(sendMessageData)		
rabbitmqObject.publishMessage(publishMessageData)
rabbitmqObject.receiveMessage(receiveMessageData)
