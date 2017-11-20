#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import hashlib

from py4j.protocol import Py4JJavaError
from pyspark.serializers import UTF8Deserializer
from pyspark.storagelevel import StorageLevel

import PahoMQTT
from pyspark.streaming import DStream

__all__ = ['MQTTUtils']


class MQTTUtils(object):
    @staticmethod
    def createStream(ssc, brokerUrl, topic,
                     storageLevel=StorageLevel.MEMORY_AND_DISK_2):
        """
        Create an input stream that pulls messages from a Mqtt Broker.

        :param ssc:  StreamingContext object
        :param brokerUrl:  Url of remote mqtt publisher
        :param topic:  topic name to subscribe to
        :param storageLevel:  RDD storage level.
        :return: A DStream object
        """

        jlevel = ssc._sc._getJavaStorageLevel(storageLevel)

        try:
            helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.streaming.mqtt.MQTTUtilsPythonHelper")
            helper = helperClass.newInstance()
            jstream = helper.createStream(ssc._jssc, brokerUrl, topic, jlevel)

        except Py4JJavaError as e:
            if 'ClassNotFoundException' in str(e.java_exception):
                MQTTUtils._printErrorMsg(ssc.sparkContext)
            raise

        # dstream =
        # dstream.serializableFunction = "None"
        # dstream.operation = "createStream"
        # dstream.additionalInformation = "brokerUrl "+brokerUrl+" topic "+topic
        #
        # dstream.addChildInDag(dstream)

        childInfo = {}
        # if hasattr(child, "prev"):
        childInfo["seqNum"] = DStream.sequenceNum
        DStream.sequenceNum += 1
        childInfo["operation"] = "createStream"
        source ={}
        source["type"] = "MQTT"
        source["address"] = brokerUrl

        source["channel"] = list(PahoMQTT.PahoMQTT.topicNames)
        childInfo["source"] = source

        childInfo["uid"] = hashlib.sha224(
            childInfo["operation"] + source["type"]+source["address"]+str(len(source["channel"]))).hexdigest()

        DStream.parentId = childInfo["uid"]
        DStream.sparkDAG.append(childInfo)
        return DStream(jstream, ssc, UTF8Deserializer())

    @staticmethod
    def _printErrorMsg(sc):
        print("""
________________________________________________________________________________________________

  Spark Streaming's MQTT libraries not found in class path. Try one of the following.

  1. Include the MQTT library and its dependencies with in the
     spark-submit command as

     $ bin/spark-submit --packages org.apache.spark:spark-streaming-mqtt:%s ...

  2. Download the JAR of the artifact from Maven Central http://search.maven.org/,
     Group Id = org.apache.spark, Artifact Id = spark-streaming-mqtt-assembly, Version = %s.
     Then, include the jar in the spark-submit command as

     $ bin/spark-submit --jars <spark-streaming-mqtt-assembly.jar> ...
________________________________________________________________________________________________
""" % (sc.version, sc.version))
