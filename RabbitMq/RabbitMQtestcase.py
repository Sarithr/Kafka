import unittest, logging
from RabbitMQclass import RabbitmqConnector
import socket

class RabbitmqTestCase(unittest.TestCase):
	def setup(self):
		logging.basicConfig(level = logging.DEBUG)
		socket.getaddrinfo('localhost', 8080)

	def testSendMessage(self):
		sendMessageData = {"queue" : "hello",
				"exchange" : "",
				"routing_key" : "hello",
				"body" : "Hello World!"}
		sendData = RabbitmqConnector("localhost", 5672)
		isMessageSent, responseMessage = sendData.sendMessage(sendMessageData)
		self.assertTrue(isMessageSent)
		self.assertEquals(responseMessage, "Message Sent Successfully")

	def testPublishMessage(self):
		publishMessageData = {"exchange" : "logs",
					"exchange_type" : "fanout",
					"routing_key" : "",
					"body" : "Hello World!"}
		publishData = RabbitmqConnector("localhost", 5672)
		isMessagePublished, responseMessage = publishData.publishMessage(publishMessageData)
		self.assertTrue(isMessagePublished)
		self.assertEquals(responseMessage, "Message Published Successfully")

	def testReceiveMessage(self):
		receiveMessageData = {"queue" : "hello",
					"on_message_callback" : "callback",
					"auto_ack" : "True"}
		receiveData = RabbitmqConnector("localhost", 5672)
		isMessageReceived, responseMessage = receiveData.receiveMessage(receiveMessageData)
		self.assertTrue(isMessageReceived)
		self.assertEquals(responseMessage, "Message Received Successfully")	

	def testSendMessageWithoutQueue(self):
		sendMessageData = {"queue" : "",
				"exchange" : "",
				"routing_key" : "hello",
				"body" : "Hello World!"}
		sendData = RabbitmqConnector("localhost", 5672)
		isMessageSent, responseMessage = sendData.sendMessage(sendMessageData)
		self.assertFalse(isMessageSent)
		self.assertEquals(responseMessage, "Provide valid queue values to sent")								

	def testSendMessageWithoutRoutingKey(self):
		sendMessageData = {"queue" : "hello",
				"exchange" : "",
				"routing_key" : "",
				"body" : "Hello World!"}
		sendData = RabbitmqConnector("localhost", 5672)
		isMessageSent, responseMessage = sendData.sendMessage(sendMessageData)
		self.assertFalse(isMessageSent)
		self.assertEquals(responseMessage, "Provide valid routing key values to sent")	

	def testSendMessageWithoutBody(self):
		sendMessageData = {"queue" : "hello",
				"exchange" : "",
				"routing_key" : "hello",
				"body" : ""}
		sendData = RabbitmqConnector("localhost", 5672)
		isMessageSent, responseMessage = sendData.sendMessage(sendMessageData)
		self.assertFalse(isMessageSent)
		self.assertEquals(responseMessage, "Provide valid body values to sent")	

	def testPublishMessageWithoutExchange(self):
		publishMessageData = {"exchange" : "",
					"exchange_type" : "fanout",
					"routing_key" : "",
					"body" : "Hello World!"}
		publishData = RabbitmqConnector("localhost", 5672)
		isMessagePublished, responseMessage = publishData.publishMessage(publishMessageData)
		self.assertFalse(isMessagePublished)
		self.assertEquals(responseMessage,  "Provide valid exchange values to publish")

	def testPublishMessageWithoutExchangeType(self):
		publishMessageData = {"exchange" : "logs",
					"exchange_type" : "",
					"routing_key" : "",
					"body" : "Hello World!"}
		publishData = RabbitmqConnector("localhost", 5672)
		isMessagePublished, responseMessage = publishData.publishMessage(publishMessageData)
		self.assertFalse(isMessagePublished)
		self.assertEquals(responseMessage, "Provide valid exchange types values to publish")

	def testPublishMessageWithoutBody(self):
		publishMessageData = {"exchange" : "logs",
					"exchange_type" : "fanout",
					"routing_key" : "",
					"body" : ""}
		publishData = RabbitmqConnector("localhost", 5672)
		isMessagePublished, responseMessage = publishData.publishMessage(publishMessageData)
		self.assertFalse(isMessagePublished)
		self.assertEquals(responseMessage,  "Provide valid body values to publish")		

	def testReceiveMessageWithoutQueue(self):
		receiveMessageData = {"queue" : "",
					"on_message_callback" : "callback",
					"auto_ack" : "True"}
		receiveData = RabbitmqConnector("localhost", 5672)
		isMessageReceived, responseMessage = receiveData.receiveMessage(receiveMessageData)
		self.assertFalse(isMessageReceived)
		self.assertEquals(responseMessage, "Provide valid queue values to receive")

	def testReceiveMessageWithoutOnMessageCallback(self):
		receiveMessageData = {"queue" : "hello",
					"on_message_callback" : "",
					"auto_ack" : "True"}
		receiveData = RabbitmqConnector("localhost", 5672)
		isMessageReceived, responseMessage = receiveData.receiveMessage(receiveMessageData)
		self.assertFalse(isMessageReceived)
		self.assertEquals(responseMessage, "Provide valid on message callback values to receive")

	def testReceiveMessageWithoutAutoAck(self):
		receiveMessageData = {"queue" : "hello",
					"on_message_callback" : "callback",
					"auto_ack" : ""}
		receiveData = RabbitmqConnector("localhost", 5672)
		isMessageReceived, responseMessage = receiveData.receiveMessage(receiveMessageData)
		self.assertFalse(isMessageReceived)
		self.assertEquals(responseMessage, "Provide valid auto ack values to receive")			