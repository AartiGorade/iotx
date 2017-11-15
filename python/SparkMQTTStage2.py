import hashlib
import json
import os
import socket
import threading
from collections import deque
from time import sleep

import paho.mqtt.client as mqtt

from pyspark import SparkContext
from pyspark.streaming import DStream
from pyspark.streaming import StreamingContext
from pyspark.streaming.mqtt import MQTTUtils

# MQTT client
mqttc = None

# Queue to store calculated average values
queue = deque([])

# Spark Broker details
sparkBroker = "iot.eclipse.org"
sparkPort = 1883
sparkTopic = "testing/spark/edu/rit/sparkDag"

# Calvin broker URI
brokerUrl = "tcp://iot.eclipse.org:1883"
# Calvin Topic pattern where temperature data is being sent
topic = "testing/calvin/edu/rit/#"


def getHostIpAddress():
    """
    Get global Ip Address of the current machine
    :return: Ip address
    """

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip = s.getsockname()[0]
    s.close()
    return ip

# Ip address and port number for Spark cluster
hostAddress = getHostIpAddress()
hostPort = "7077"

def connectToBroker(broker, port):
    """
    This is the function responsible for creating MQTT client and connecting to the give broker server on desired port
    :param broker: broker server
    :param port: port to connect to
    :return: None
    """

    global mqttc
    mqttc = mqtt.Client()
    print "Trying to connect to broker..."
    mqttc.connect(broker, port)
    print "Successfully connected!!!"

    childInfo = {}
    # if hasattr(child, "prev"):
    childInfo["seqNum"] = DStream.sequenceNum
    DStream.sequenceNum += 1
    childInfo["operation"] = "publish"
    sink ={}
    sink["type"] = "MQTT"
    sink["address"] = sparkBroker+":"+str(sparkPort)
    sink["channel"] = sparkTopic
    childInfo["sink"] = sink
    childInfo["parent"] = DStream.parentId

    childInfo["uid"] = hashlib.sha224(
        childInfo["operation"] + sink["type"]+sink["address"]+ sink["channel"]+childInfo["parent"]).hexdigest()

    DStream.parentId = childInfo["uid"]
    DStream.sparkDAG.append(childInfo)


def extractDag():
    """
    Extract Directed Acyclic graph and offload to Edge in JSON format
    :return:
    """

    return json.dumps(DStream.sparkDAG)

def addToQueue():
    """
    This is the function responsible for adding extracted DAG JSON into the queue
    :return: None
    """

    # Keep adding newly extracted DAG JSON in queue
    while True:
        global queue
        queue.append(extractDag())
        # wait for 5 seconds before queueing next DAG JSON
        sleep(5)

def publishFromQueue():
    """
    This is the function responsible for fetching data from queue and publishing it using MQTT
    :return: None
    """

    global queue
    # keep publishing DAG JSON
    while True:
        #wait for 15 seconds before publishing next DAG JSON
        while not (queue):
            sleep(15)

        data = queue.popleft()
        print(data)
        mqttc.publish(sparkTopic, data)


def printSparkDAG():
    """
    This is the function responsible to print extracted DAG in JSON readable format
    Note: required to modify to support Source and Sink JSON
    :return: None
    """

    numEvents = len(DStream.sparkDAG)
    print("[")
    for i in range(0, numEvents):
        event = DStream.sparkDAG[i]
        print(" {")
        print "   seqNum: ", event["seqNum"]
        print "   rddType: ", event["rddType"]
        print "   operationType: ", event["operationType"]
        print "   operation: ", event["operation"]
        print "   closure: ", event["closure"]
        print "   additional Information: ", event["additionalInformation"]
        print "   parent: ", event["parent"]
        print "   uid: ", event["uid"]
        print(" }")
    print("]")


if __name__ == "__main__":
    '''
    This is the main function responsible for collecting DAG from Spark and off loading to Calvin client
    to perform evaluation
    '''

    # Load spark streaming mqtt package at runtime
    SUBMIT_ARGS = "--packages org.apache.bahir:spark-streaming-mqtt_2.11:2.2.0 pyspark-shell"
    os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS

    # connect to Spark cluster "spark:cluster-host:port"
    #sc = SparkContext("spark://129.21.124.229:7077", appName="iotx-stage2")
    # connect to Spark cluster "spark:cluster-host:port"
    sc = SparkContext("spark://"+hostAddress+":"+hostPort, appName="iotx")
    sc.setLogLevel("ERROR")

    print("Created Streaming context...")
    # reading data every 15 seconds
    ssc = StreamingContext(sc, 15)

    # mandatory to store checkpointed data for Spark Streaming
    ssc.checkpoint("/Users/Aarti/IdeaProjects/SparkCheckpointedData")

    print("Creating MQTT stream...")
    mqttStream = MQTTUtils.createStream(ssc, brokerUrl, topic)

    # split incoming stream based on space
    celsiusTemp = mqttStream.map(lambda line: line.split(" "))

    # Convert Celsius to Farenheit and store each value in pair format
    farenheitTemp = celsiusTemp.map(lambda temp: (str((float(temp[0]) * 9 / 5) + 32).decode("utf-8"), 1))

    # perform print action
    farenheitTemp.pprint()

    # connect to broker
    connectToBroker(sparkBroker, sparkPort)

    # Worker thread to perform operation to add newly extracted data into queue
    t = threading.Timer(10.0, addToQueue)
    t.start()

    # Get DAG JSON from queue and publish to broker for Calvin usage
    publishFromQueue()