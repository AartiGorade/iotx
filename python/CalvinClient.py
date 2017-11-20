# echo_client.py
import json
import socket

import base64
import dill


# from python.pyspark import SparkContext
# from python.pyspark import rdd
# from python.pyspark.sql import SQLContext
# from python.pyspark.sql.types import *
# from python.pyspark.mllib.common import _py2java, _java2py


def collectDAGFromSpark():
    '''
    :return: sparkDag Deserialized Directed Acyclic graph information
    This function is called to collect Directed Acyclic graph from Spark. Socket is opened and server with
    known hostname or Ip address along with port number is connected to receive DAG information in JSON format.
    After Successful deserialization Calvin client sends Success response, else send out failure
    '''

    # port to connect to Server
    port = 12345
    # Open Socket
    s = socket.socket()
    # Connect to server with destined port
    s.connect(('127.0.0.1', port))

    print "Connected to SPARK server"
    # receive data from server, here maximum 4096 bytes can be received
    data = s.recv(4096)

    print "\nDeserializing SPARK DAG into JSON..."
    # deserialize using json
    try:
        sparkDag = json.loads(data)
        # send positive response to server after successful deserialization
        s.send("success")
    except:
        s.send("failure")

    # close the socket
    s.close()

    return sparkDag


def evaluateSparkDAG(sparkDag):
    '''
    :param sparkDag: Deserialized Directed Acyclic graph (DAG) information
    :return: Resilient Distributed Dataset(RDD) after application of received DAG.
    This function is called to mimic Calvin client side. The received Spark DAG is parsed
    to perform type of operations in given sequence. Also, closure information is applied
    along with the operation
    '''

    print("\nStarting evaluation of SPARK DAG...\n")
    # sc = SparkContext('local')
    # print ("\nSpark DAG =>")
    # print (sparkDag)
    numRDDs = len(sparkDag) - 1
    rdd = None

    closureList = dill.loads(base64.b64decode(sparkDag[numRDDs]["closure"].encode()))
    print("Number of inputs: ",closureList.func_code.co_argcount)

    # DAG is parsed and corresponding operation is chosen to apply closure on particular RDD
    # for i in range(numRDDs, -1, -1):
    #     event = sparkDag[i]
    #     closureList = dill.loads(base64.b64decode(event["closure"].encode()))
    #     if "textFile" in event["parent"]:
    #         inputSource = event["parent"].split(" ")[0]
    #         text_file = sc.textFile(inputSource)
    #         if (event["operation"] == "map"):
    #             rdd = text_file.map(closureList)
    #
    #     elif "PythonRDD" in event["parent"]:
    #         if (event["operation"] == "map"):
    #             rdd = rdd.map(closureList)

    # resultant RDD
    return rdd


if __name__ == "__main__":
    '''
    This is the main function responsible for collecting DAG from Spark and invoking DAG evaluation 
    at Calvin side to collect results
    '''
    sparkDag = collectDAGFromSpark()
    rdd = evaluateSparkDAG(sparkDag)
    print("\n\nResult =>")
    print(rdd.collect())
