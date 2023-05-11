#Import libraries
import utime,time,machine,ujson,network
from umqtt.simple import MQTTClient
from machine import Pin, UART, I2C
from imu import MPU6050
from time import sleep

#Initia2alize variables
rtc = machine.RTC()

#GPS NEO 6M initialization
gpsModule = UART(1, baudrate=9600, tx=Pin(4), rx=Pin(5))

buff = bytearray(255)

FIX_STATUS = False

latitude = ""
longitude = ""
satellites = ""
GPStime = ""

placa = 'ABC-123'

#Wifi credentials
ssid = 'HARVARD_plus'
password = 'Samuelex'

wlan = network.WLAN(network.STA_IF)
wlan.active(True)
wlan.connect(ssid, password)
 
# Wait for connect or fail
max_wait = 10
while max_wait > 0:
    if wlan.status() < 0 or wlan.status() >= 3:
        break
    max_wait -= 1
    print('waiting for connection...')
    time.sleep(1)

# Handle connection error
if wlan.status() != 3:
    raise RuntimeError('network connection failed')
else:
    print('connected')
    status = wlan.ifconfig()
    print( 'ip = ' + status[0] )

#MPU6050 initialization
i2c = I2C(0, sda=Pin(0), scl=Pin(1), freq=400000)
imu = MPU6050(i2c)

#Azure IoT Hub credentials
hostname = 'riskstreamhub.azure-devices.net'
clientid = 'riskdevice'
user_name = 'riskstreamhub.azure-devices.net/riskdevice/?api-version=2021-04-12'
passw = 'SharedAccessSignature sr=riskstreamhub.azure-devices.net%2Fdevices%2Friskdevice&sig=yvPIrh9gnIvLey5DG7O70VOhZWP17IVHBndWtQmNvFY%3D&se=2283256369'
topic_pub = b'devices/riskdevice/messages/events/'
port_no = 0
subscribe_topic = "devices/riskdevice/messages/devicebound/#"

#Get GPS location
def getGPS(gpsModule):
    global FIX_STATUS, TIMEOUT, latitude, longitude, satellites, currentdate,currenttime
    
    try:
        #Get the value every 1 second
        timeout = utime.ticks_ms() + 1000
        
        #Try to get the value 4 times in that second
        while True:
            #Get the date and time
            year, month, day, weekday, hour, minute, second, subsecond = rtc.datetime()
            currentdate = "{:04}-{:02}-{:02}".format(year, month, day)
            currenttime = "{:02}:{:02}:{:02}".format(hour, minute, second)
            
            #Get the GPS data
            gpsModule.readline()           
            buff = str(gpsModule.readline())
            parts = buff.split(',')
            #Only get the GPGGA data found for the latitude and longitude
            if ((parts[0] == "b'$GPGGA") and (len(parts) == 15)):
                if(parts[1] and parts[2] and parts[3] and parts[4] and parts[5] and parts[6] and parts[7]):
                    latitude = convertToDegree(parts[2])
                    if (parts[3] == 'S'):
                        latitude = '-' + latitude
                    longitude = convertToDegree(parts[4])
                    if (parts[5] == 'W'):
                        longitude = '-' + longitude
                    FIX_STATUS = True
                    
            if (utime.ticks_ms() > timeout):
                #if 1 second is reached, send the location found
                break
            utime.sleep_ms(250)
            
    except ValueError as e:
        #if error in the data is found, just break the loop
        print("Invalid input:", e)
        pass
        
def convertToDegree(RawDegrees):
    #Convert to degrees the location
    RawAsFloat = float(RawDegrees)
    firstdigits = int(RawAsFloat/100) 
    nexttwodigits = RawAsFloat - float(firstdigits*100) 
    Converted = float(firstdigits + nexttwodigits/60.0)
    Converted = '{0:.4f}'.format(Converted)
    return str(Converted)

def mqtt_connect():
    #Connect to Azure IoT Hub with a baltimore certificate
    certificate_path = "baltimore.cer"
    print('Loading Blatimore Certificate')
    with open(certificate_path, 'r') as f:
        cert = f.read()
    print('Obtained Baltimore Certificate')
    sslparams = {'cert':cert}
    client = MQTTClient(client_id=clientid, server=hostname, port=port_no, user=user_name, password=passw, keepalive=3600, ssl=True, ssl_params=sslparams)
    client.connect()
    print('Connected to IoT Hub MQTT Broker')
    return client

def reconnect():
    #Try to reconnect via MQTT
    print('Failed to connect to the MQTT Broker. Reconnecting...')
    time.sleep(5)
    machine.reset()

def callback_handler(topic, message_receive):
    #Callback Handler
    print("Received message")
    print(message_receive)

try:
    #Try Connect via MQTT
    client = mqtt_connect()
    client.set_callback(callback_handler)
    client.subscribe(topic=subscribe_topic)
except OSError as e:
    reconnect()

#Main loop
while True:
    #Obtain GPS data
    getGPS(gpsModule)
    
    #If you get the location at least one, get the other values
    if(FIX_STATUS == True):
        #Get calibrated acceleration
        ay=imu.accel.y
        ay_offset = -1.00221 * ay + 0.02480005
        ay_with_offset = ay - ay_offset
        #Continue only if car is moving
        if abs(ay_with_offset)>0.01:
            #if absolute acceleration is smaller than 0.5,convert it to 0, else send current acceleration
            if abs(ay_with_offset) > 0.5:
                #convert to m/s2 acceleration
                acceleration = ay_with_offset * 9.81
            else:
                acceleration = 0
            
            #construct message to send
            msg = {
                "placa": str(placa),
                "acceleration":str(acceleration),
                "latitude":str(latitude),
                "longitude":str(longitude),
                "date":str(currentdate),
                "time":str(currenttime),
                }
            print(msg)
            #Send message to Azure IoT Hub
            topic_msg = ujson.dumps(msg)
            client.check_msg()
            client.publish(topic_pub, topic_msg)
        else:
            #If the first location is not found, do not send data
            pass