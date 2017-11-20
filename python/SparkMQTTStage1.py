########################################################################
# This is implementation for Cloud + Edge = iotx Stage 1. Cloud is represented
# by Apache Spark and Edge computing framework is Calvin. Apache Spark is
# receiving temperature data from Calvin via MQTT (pub/sub model). This
# program calculates running average using windowing and sliding interval
# technique and sends the result back to Calvin via MQTT
#
# iotx stage 1 demo
#
# Author: Aarti Gorade
# Email: ahg1512@rit.edu
#
# Invocation:
#
# Docker image: aarti/sparkstage1-iotx
# Docker file: DockerfileSparkMQTTStage1
#
# OR
#
# Command line:
#   ./sbin/start-master.sh
#   ./bin/spark-class org.apache.spark.deploy.worker.Worker spark://<Spark
# Master's Ip address>:<Spark Master's Port>
#   ./bin/spark-submit
# --packages org.apache.spark:spark-streaming-mqtt-assembly_2.11:1.5.0
# python/SparkMQTTStage1.py
#
########################################################################

import os
import socket
from collections import deque
from threading import Thread
from time import sleep

import paho.mqtt.client as mqtt

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.mqtt import MQTTUtils

# MQTT client
mqttc = None

# Queue to store calculated average values
queue = deque([])

# Spark Broker details
sparkBroker = "iot.eclipse.org"
sparkPort = 1883
sparkTopic = "edu/rit/iotx/cloud/average/temperature"

# Calvin broker URI
brokerUrl = "tcp://iot.eclipse.org:1883"
# Topic pattern where temperature data is being sent
topic = "edu/rit/iotx/+/temperature"

# counters to keep track of running sum and count to calculate average value
sumAccum = 0
countAccum = 0

# window and sliding interval using for calculating average over each window of
# incoming Spar Stream
windowInterval = 30
slidingInterval = 15


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
    This is the function responsible for creating MQTT client and connecting to
    the give broker server on desired port
    :param broker: broker server
    :param port: port to connect to
    :return: None
    """

    global mqttc
    mqttc = mqtt.Client()
    print "Trying to connect to broker..."
    mqttc.connect(broker, port)
    print "Successfully connected!!!"


def addToQueue(rdd):
    """
    This is the function responsible for adding calculated average values into
    the queue
    :param rdd: RDD containing calculated average values
    :return: None
    """

    rddList = rdd.collect()
    subList = [float(x[0]) for x in rddList]
    global queue
    queue.extend(subList)


def publishFromQueue():
    """
    This is the function responsible for fetching data from queue and publishing it using MQTT
    :return: None
    """

    global mqttc
    global queue
    mqttClient = mqttc

    while True:
        while not (queue):
            sleep(slidingInterval)

        data = queue.popleft()
        print(data)
        mqttClient.publish(sparkTopic, data)


def update(x):
    """
    Add the incoming new item in current sliding window interval into the sum
    :param x: new value
    :return: current average value
    """

    global sumAccum
    global countAccum
    sumAccum += x
    countAccum += 1
    return (sumAccum / countAccum)


def reverseUpdate(x):
    """
    Remove item from old sliding window interval from current sum
    :param x: old item from last window interval
    :return: current average value
    """

    global sumAccum
    global countAccum
    sumAccum -= x
    countAccum -= 1
    return (sumAccum / countAccum)


if __name__ == "__main__":
    """
    This is the main function responsible for calculating average of input data 
    stream pe window and publishing calculated average values for Calvin client 
    usage to perform further processing using Sensors or Actuators
    """

    # Load spark streaming mqtt package at runtime
    SUBMIT_ARGS = "--packages " \
                  "org.apache.spark:spark-streaming-mqtt-assembly_2.11:1.5.0 " \
                  "pyspark-shell"
    os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS

    # connect to Spark cluster "spark:cluster-host:port"
    sc = SparkContext("spark://" + hostAddress + ":" + hostPort, appName="iotx")
    sc.setLogLevel("ERROR")

    print("Created Streaming context...")
    ssc = StreamingContext(sc, 15)

    # mandatory to store checkpointed data for Spark Streaming
    # temp
    ssc.checkpoint("/Users/Aarti/IdeaProjects/SparkCheckpointedData")

    print("Creating MQTT stream...")
    mqttStream = MQTTUtils.createStream(ssc, brokerUrl, topic)

    # split incoming stream based on space
    celsiusTemp = mqttStream.map(lambda line: line.split(" "))

    # Convert Celsius to Farenheit and store each value in pair format
    farenheitTemp = celsiusTemp.map(
        lambda temp: (str((float(temp[0]) * 9 / 5) + 32).decode("utf-8"), 1))

    # lambda functions to calculate average using windowing technique
    update_1 = lambda x, y: update(x)
    reverseUpdate_1 = lambda x, y: reverseUpdate(x)

    # Reduce last 30 seconds of data, every 15 seconds
    windowedWordCounts = farenheitTemp.reduceByKeyAndWindow(update_1,
                                                            reverseUpdate_1,
                                                            windowInterval,
                                                            slidingInterval)

    # connect to broker
    connectToBroker(sparkBroker, sparkPort)

    # foreachRDD is Action. Add each RDD containing average values into the
    # queue
    windowedWordCounts.foreachRDD(addToQueue)

    # create worker thread to fetch data from queue and publish it to broker
    # using MQTT
    worker = Thread(target=publishFromQueue)
    worker.setDaemon(True)
    worker.start()

    # Start spark streaming jobs
    print("\n\n SSC starting ...")
    ssc.start()
    print("\n\n SSC waiting for termination...")

    # wait for 100 seconds before terminating Spark job execution
    ssc.awaitTermination()
