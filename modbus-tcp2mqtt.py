#!/usr/bin/python3
# Simple MQTT publishing of Modbus TCP sources
#
# Written and (C) 2018 by Lubomir Kamensky <lubomir.kamensky@gmail.com>
# Provided under the terms of the MIT license
#
# Requires:
# - pyModbusTCP - https://github.com/sourceperl/pyModbusTCP
# - Eclipse Paho for Python - http://www.eclipse.org/paho/clients/python/
#   frequency before is 30,now change to 5*60=300
import json
import _thread
import datetime
import threading
import argparse
import logging
import logging.handlers
import time
import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as subscribe
import sys
import configparser
import traceback
from pyModbusTCP.client import ModbusClient

parser = argparse.ArgumentParser(description='Bridge between Modbus TCP and MQTT')
parser.add_argument('--mqtt-host', default='localhost', help='MQTT server address. \
                     Defaults to "localhost"')
parser.add_argument('--mqtt-port', default='8883', type=int, help='MQTT server port. \
                    Defaults to 8883')
parser.add_argument('--mqtt-topic', default='', help='Topic prefix to be used for \
                    subscribing/publishing. Defaults to "modbus/"')
parser.add_argument('--modbus-host', help='Modbus server address')
parser.add_argument('--modbus-port', default='502', type=int, help='Modbus server port. \
                    Defaults to 502')
parser.add_argument('--registers', help='Register definition file. Required!')
parser.add_argument('--frequency', default='50', help='How often is the source \
                    checked for the changes, in seconds. Only integers. Defaults to 3')
parser.add_argument('--only-changes', default='False', help='When set to True then \
                    only changed values are published')

args = parser.parse_args()

# logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("log.txt")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# 把logger加到其他地方。
#logger.info("Start print log")

topic = args.mqtt_topic
# if not topic.endswith("/"):
#    topic += "/"
frequency = int(args.frequency)
print("ModBusTcp2Mqtt App Started...Time:%s" % (datetime.datetime.now()))
mbClient = None
lastValue = {}

config = configparser.ConfigParser()

config.read(args.registers)

config01 = config['0x01']
config02 = config['0x02']
config03 = config['0x03']
config04 = config['0x04']
config05 = config['0x05']
config06 = config['0x06']
config0F = config['0x0F']
config10 = config['0x10']
config2B = config['0x2B']


# Any received value in the upper range (32768-65535)
# is interpreted as negative value (in the range -32768 to -1). 
def reMap(value, maxInput=65535, minInput=64535, maxOutput=-1, minOutput=-1001):
    # if value >= minInput:
    #     value = maxInput if value > maxInput else value
    #     value = minInput if value < minInput else value
    #
    #     inputSpan = maxInput - minInput
    #     outputSpan = maxOutput - minOutput
    #
    #     scaledThrust = float(value - minInput) / float(inputSpan)
    #
    #     return minOutput + (scaledThrust * outputSpan)
    # else:
    return value


class Element:
    def __init__(self, row):
        self.topic = row[0]
        self.value = row[1]

    def publish(self):
        try:
            if self.value != lastValue.get(self.topic, 0) or args.only_changes == 'False':
                lastValue[self.topic] = self.value
                fulltopic = topic + self.topic
                ## mqClient.subscribe(fulltopic)
                mqClient.publish(fulltopic, reMap(self.value), qos=1, retain=False)
        except Exception as exc:
            logging.info("Error reading " + self.topic + ": %s", exc)


def readMb():
    #while True:
        # open or reconnect TCP to server
        logger.info("readMb Run...Time:%s" % (datetime.datetime.now()))
        if not mbClient.is_open():
            if not mbClient.open():
                logging.error("unable to connect to " + SERVER_HOST + ":" + str(SERVER_PORT))

        data = []

        for key, value in config01.items():
            # 读取时增加对指令key的过滤
            if mbClient.is_open() and not str(key).__contains__('command'):
                row = mbClient.read_coils(int(value))
                if not row is None:
                    row.insert(0, key)
                    data.append(row)

        for key, value in config02.items():
            if mbClient.is_open() and not str(key).__contains__('command'):
                row = mbClient.read_discrete_inputs(int(value))
                if not row is None:
                    row.insert(0, key)
                    data.append(row)

        for key, value in config03.items():
            if mbClient.is_open() and not str(key).__contains__('command'):
                row = mbClient.read_holding_registers(int(value))
                if not row is None:
                    row.insert(0, key)
                    data.append(row)
        for key, value in config04.items():
            if mbClient.is_open() and not str(key).__contains__('command'):
                row = mbClient.read_input_registers(int(value))
                if not row is None:
                    row.insert(0, key)
                    data.append(row)
        for row in data:
            e = Element(row)
            e.publish()
        #time.sleep(int(frequency))
        global timer
        timer = threading.Timer(120, readMb)
        timer.start()
        logger.info("readMb Started...Time:%s" % (datetime.datetime.now()))


# 指令回执发回mqtt
def down_back(result):
    callback_topic = "command/huailaiwaterworks/one/downback"
    mqClient.publish(callback_topic, result, qos=1, retain=False)
    print("publishCallBack-Msg:%s" % result)
    logger.info("publishCallBack-Msg:%s" % result)

# call back msg
def msgCallback():
    def on_message_print(client, userdata, message):
        msg = str(message.payload)
        newstr = msg.strip('b')
        print("callback msg:%s" % (newstr))
        logger.info("callback msg:%s" % (newstr))
        if not mbClient.is_open():
            if not mbClient.open():
                logging.error("unable to connect to " + SERVER_HOST + ":" + str(SERVER_PORT))
                mbClient.open()
                print("reconnected to modbus finished...")
        # 水厂泵开关
        for key, value in config01.items():
            if mbClient.is_open():
                if newstr == key:
                    row = mbClient.read_coils(int(value))
                    print("coils-read-back:%s" % (row))
                    logger.info("coils-read-back:%s" % (row))
                    result1 = mbClient.write_single_coil(int(value), True)
                    row = mbClient.read_coils(int(value))
                    print("coils-write-back1:%s ,NOW Status:%s" % (result1, row))
                    logger.info("coils-write-back1:%s ,NOW Status:%s" % (result1, row))
                    time.sleep(2)  # PLC反应时间
                    result2 = mbClient.write_single_coil(int(value), False)
                    row = mbClient.read_coils(int(value))
                    # 执行回执，也publish出去；
                    if result1 is not None:
                        if result1:
                            down_back(newstr + '/0000')

                    if result1 is None or row is None:
                        down_back(newstr + '/9999')

                    print("coils-write-back2:%s,NOW Status:%s" % (result2, row))
                    print(key + ":coils-operation-over...")
                    logger.info("coils-write-back2:%s,NOW Status:%s" % (result2, row))
                    logger.info(key + ":coils-operation-over...")
        # 寄存器加压站 井
        for key, value in config03.items():
            if mbClient.is_open():
                if newstr == key:
                    # 根据topic 构造write value
                    # 加压站   确定首位是不是零位。
                    # 地址顺序有误， 1号---会开4号;3号---会开1号；4号---3号，所以做调整
                    if 'station_pump4#start' in newstr:
                        write_value = 2
                    if 'station_pump4#stop' in newstr:
                        write_value = 4

                    if 'station_pump2#start' in newstr:
                        write_value = 8
                    if 'station_pump2#stop' in newstr:
                        write_value = 16

                    if 'station_pump1#start' in newstr:
                        write_value = 32
                    if 'station_pump1#stop' in newstr:
                        write_value = 64

                    if 'station_pump3#start' in newstr:
                        write_value = 128
                    if 'station_pump3#stop' in newstr:
                        write_value = 256

                    # 井 保留联动，优化选择到井的条件
                    if 'command/well' in newstr and 'pump#start' in newstr:
                        write_value = 1
                    if 'command/well' in newstr and 'pump#stop' in newstr:
                        write_value = 2
                    if 'command/well' in newstr and 'pump#linkact' in newstr:
                        write_value = 4

                    row = mbClient.read_holding_registers(int(value))
                    print("holding-Register-read-back:%s" % (row))
                    logger.info("holding-Register-read-back:%s" % (row))
                    result1 = mbClient.write_single_register(int(value), write_value)
                    row = mbClient.read_holding_registers(int(value))
                    print("holding-Register-write-back1:%s ,addr:%s ,writeValue:%s,NOW value:%s" % (
                    result1, value, write_value, row))
                    logger.info("holding-Register-write-back1:%s ,addr:%s ,writeValue:%s,NOW value:%s" % (
                        result1, value, write_value, row))
                    time.sleep(2)
                    result2 = mbClient.write_single_register(int(value), 0)
                    row = mbClient.read_holding_registers(int(value))

                    if result1 is not None:
                        if result1:
                            down_back(newstr + '/0000')

                    if result1 is None or row is None:
                        down_back(newstr + '/9999')
                    print("holding-Register-write-back2:%s,NOW Status:%s" % (result2, row))
                    print(key + ":holding-Register-operation-over...")

                    logger.info("holding-Register-write-back2:%s,NOW Status:%s" % (result2, row))
                    logger.info(key + ":holding-Register-operation-over...")
    subscribe.callback(on_message_print, command_topic, hostname="39.107.156.237")


try:
    mqClient = mqtt.Client()
    # mqClient.connect("MacBook-Air.local", 1883) 上线时可以还原为这个地址
    mqClient.connect("39.107.156.237", 1883)
    mqClient.tls_set("cacert.pem", "client-cert.pem", "client-key.pem")
    mqClient.loop_start()
    # 订阅指令topic 增加topic复杂度，防止误触发
    command_topic = "huailaiwater/ESLink/prod/command/"
    mqClient.subscribe(command_topic)
    print("SUBCRIBE " + command_topic + " Successfully")
    mbClient = ModbusClient()
    # define modbus server host, port
    SERVER_HOST = args.modbus_host
    SERVER_PORT = args.modbus_port
    mbClient.host(SERVER_HOST)
    mbClient.port(SERVER_PORT)

    # 启动读modbus与订阅指令线程
   # _thread.start_new_thread(readMb, ())
    readMb()
    _thread.start_new_thread(msgCallback, ())


except Exception as e:
    #  traceback.print_exc()+
    logging.error("Unhandled error [" + str(e) + traceback.print_exc() + "]")
    sys.exit(1)
while 1:
    pass
