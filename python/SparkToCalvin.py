import json
import socket

from pyspark import SparkContext
from pyspark.rdd import RDD


def printSparkDAG():
    numEvents = len(RDD.sparkDAG)
    print("[")
    for i in range(0, numEvents):
        event = RDD.sparkDAG[i]
        print(" {")
        print "   id: ", event["id"]
        print "   operation: ", event["operation"]
        print "   closure: ", event["closure"]
        print "   parent: ", event["input"]
        print "   uid: ", event["uid"]
        print(" }")
    print("]")


def createDAG(child):
    '''
    :param child: The child node till where Directed Acyclic Graph processing is required to off load to Calvin
    :return: sparkDag Serialized Directed Acyclic Graph from root till given child node
    This function is called to create Directed Acyclic graph from Spark. Part of large DAG graph from root node
    till provided child node is formulated and stored in the JSON format for transfer to Calvin client
    '''
    sparkDAG = []
    id = 0
    print("\nDAG created => \n")
    while child != None:
        print(child)
        print(type(child))
        childInfo = {}
        if hasattr(child, "prev"):
            childInfo["id"] = id
            childInfo["operation"] = child.operation
            childInfo["closure"] = child.serializableFunction
            childInfo["parent"] = str(child.prev)
            print("id ", childInfo["id"])
            print("operation ", childInfo["operation"])
            print("closure ", childInfo["closure"])
            print("parent ", childInfo["parent"])
            child = child.prev
            sparkDAG.append(childInfo)
            id += 1;
        else:
            return sparkDAG


def transferJsonToCalvin(sparkDagJson):
    '''
    :param sparkDagJson: Serialized Directed Acyclic Graph
    :return: sparkDag Serialized Directed Acyclic Graph from root till give child node
    This function is called to create Directed Acyclic graph from Spark. Part of large DAG graph from root node
    till provided child node is formulated and stored in the JSON format for transfer to Calvin client
    '''

    # Socket is opened with desired non reserved port
    port = 12345
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', port))
    print "socket binded to %s" % (port)
    # Socket is listening for max 5 connections, 6th connection is rejected
    s.listen(5)
    print "socket is listening"
    while True:
        # Accept connections from Calvin clients
        clientConnection, clientAddress = s.accept()
        print('Connection received from', clientAddress)

        # Send serialized Spark DAG to Calvin client
        clientConnection.sendall(sparkDagJson)

        # receive response from Calvin client
        response = clientConnection.recv(1024)

        # If positive response received from client, close the socket and client connection too
        # else close only client connection
        if (response == "success"):
            print("\nDAG transfer from Spark to Calvin is successful!!!")
            clientConnection.close()
            s.close()
            break;
        else:
            print("\nDAG transfer from Spark to Calvin has failed. Please try again!!!")
            clientConnection.close()


if __name__ == "__main__":
    '''
    This is the main function responsible for collecting DAG from Spark and off loading to Calvin client
    to perform evaluation
    '''

    sc = SparkContext("local", appName="iotx")

    #### *****************************
    text_file = sc.textFile("/Users/Aarti/Documents/Fall2017/Code/spark-2.2.0/Input.txt")
    # text_file = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

    convertFunc = lambda x,y: (float(x+y) - 32) * .5556

    mapRDD_1 = text_file.map(convertFunc)

    # mapRDD_1.mean()

    countFunc = lambda word: (word, 1)
    mapRDD_2 = mapRDD_1.map(countFunc)
    # print mapRDD_2.collect()
    # print mapRDD_1.collect()

    # printSparkDAG()
    #
    # print "Creating SPARK DAG..."
    # sparkDag = createDAG(mapRDD_2)

    print "\nConverting SPARK DAG into JSON..."
    sparkDagJson = json.dumps(RDD.sparkDAG)

    # with open('SparkDAGJson.txt', 'w') as outfile:
    #     json.dump(RDD.sparkDAG, outfile)


    print "Transferring DAG from SPARK to Calvin...\n"
    transferJsonToCalvin(sparkDagJson)

        #### *****************************
        # print mapRDD_2.collect()
        # val = mapRDD_2.reduce(lambda accum, n: accum + n)
        # print("val = ",val)

        # print(inspect.getsource(hello))

        # print(inspect.getsource(splitFunc))
