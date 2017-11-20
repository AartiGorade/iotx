########################################################################
# This is part of implementation for Cloud + Edge = iotx Stage 2. Cloud is
# represented by Apache Spark and Edge computing framework is Calvin. This
# code focuses on subscribing to MQTT topic of Calvin, collect data and keep
# storing unique topic names corresponding to the receieved data
#
# iotx stage 2 demo
#
# Author: Aarti Gorade
# Email: ahg1512@rit.edu
#
#
########################################################################


import paho.mqtt.client as mqtt


class PahoMQTT(mqtt.Client):
    """
    Paho mqtt client to connect to MQTT server to received data being
    published by Calvin and collects all different topic names
    """

    # set to store all unique topic names
    topicNames = set()

    def on_message(self, mqttc, obj, msg):
        """
        Add topic name to the set when the data is received from MQTT server
        :param mqttc: mqtt client
        :param obj: received object
        :param msg: data mqtt payload
        :return: None
        """
        PahoMQTT.topicNames.add(msg.topic)

    def run(self, broker, port, topic):
        """
        Connect to the MQTT broker and subscribe to the the topic to receive
        the data being published by Calvin continuously
        :return:
        """
        self.connect(broker, port, 60)
        self.subscribe(topic, 0)

        rc = 0
        while rc == 0:
            rc = self.loop()
        return rc
