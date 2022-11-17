#Importamos librerías
from time import sleep
import os,sys
import RPi.GPIO as GPIO
import paho.mqtt.client as paho
from urllib.parse import urlparse
import datetime
import boto3
import threading
#Iniciamos puertos
GPIO.setmode(GPIO.BOARD)
GPIO.setwarnings(False)
LED_PIN_0=11  #define LED pin - Adelante
LED_PIN_1=12  #define LED pin - Atras
GPIO.setup(LED_PIN_0,GPIO.OUT)   # Set pin function as output
GPIO.setup(LED_PIN_1,GPIO.OUT)   # Set pin function as output
global counter

class MyDb(object):

    def __init__(self, Table_Nomen='DHT'):
        self.Table_Name=Table_Nomen

        self.db = boto3.resource('dynamodb')
        self.table = self.db.Table(Table_Nomen)

        self.clientis = boto3.client('dynamodb')

    @property
    def get(self):
        response = self.table.get_item(
            Key={
                'Sensor_Id':"1"
            }
        )

        return response

    def put(self, Sensor_Id='' , Adelante='', Atras=''):
        self.table.put_item(
            Item={
                'Sensor_Id':Sensor_Id,
                'Adelante':Adelante,
                'Atras' :Atras
            }
        )

    def delete(self,Sensor_Id=''):
        self.table.delete_item(
            Key={
                'Sensor_Id': Sensor_Id
            }
        )

    def describe_table(self):
        response = self.clientis.describe_table(
            TableName='Sensor'
        )
        return response

  
def scribereDB(adelante,atras):
    threading.Timer(interval=10, function=main).start()
    obj = MyDb()
    obj.put(Sensor_Id=str(counter), Adelante=str(adelante), Atras=str(atras))
    counter = counter + 1
    print("Sample in Cloud uploaded:{},Atras{} ".format(adelante, atras))


def on_connect(self, mosq, obj, rc):
        self.subscribe("led", 0)
    
def on_message(mosq, obj, msg):
    counter = 0
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
    if(msg.payload == "on"): 
        #se crea botón on   
        print("Ducitur on")      
        GPIO.output(LED_PIN_0,GPIO.HIGH)  #Adelante
        GPIO.output(LED_PIN_1,GPIO.LOW)  #Atras
        scribereDB("1","0")
    else:  
        #se crea botón off  
        print("Ducitur off")  
        GPIO.output(LED_PIN_0,GPIO.LOW)   #Adelante
        GPIO.output(LED_PIN_1,GPIO.HIGH)  #Atras
        scribereDB("0","1")

def on_publish(mosq, obj, mid):
    print("mid: " + str(mid))

    
def on_subscribe(mosq, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


def main():

    mqttc = paho.Client()  # object declaration
    # Assign event callbacks
    mqttc.on_message = on_message  # called as callback
    mqttc.on_connect = on_connect
    mqttc.on_publish = on_publish
    mqttc.on_subscribe = on_subscribe



    url_str = os.environ.get('CLOUDMQTT_URL', 'tcp://currus.udem.io:1883') 
    url = urlparse(url_str)
    mqttc.connect(url.hostname, url.port)

    rc = 0
    while True:
        while rc == 0:
            import time   
            rc = mqttc.loop()
            #time.sleep(0.5)
        print("rc: " + str(rc))

main()