
class DagModification:

    stopDagModification = False

    # ssc = None

    # def getHostIpAddress(self):
    #     """
    #     Get global Ip Address of the current machine
    #     :return: Ip address
    #     """
    #
    #     s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #     s.connect(("8.8.8.8", 80))
    #     ip = s.getsockname()[0]
    #     s.close()
    #     return ip

    # def createSparkContext(self):
    #
    #     global ssc
    #
    #     if(ssc is not None):
    #         # Ip address and port number for Spark cluster
    #         hostAddress = self.getHostIpAddress()
    #         hostPort = "7077"
    #
    #         # connect to Spark cluster "spark:cluster-host:port"
    #         sc = SparkContext("spark://" + hostAddress + ":" + hostPort, appName="iotx")
    #         sc.setLogLevel("ERROR")
    #
    #         print("Created Streaming context...")
    #         # reading data every 15 seconds
    #         ssc = StreamingContext(sc, 15)
    #
    #         # mandatory to store checkpointed data for Spark Streaming
    #         ssc.checkpoint("/Users/Aarti/IdeaProjects/SparkCheckpointedData")
    #
    #     return ssc