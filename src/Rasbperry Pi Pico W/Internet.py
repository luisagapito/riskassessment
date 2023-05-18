import time
import network
 
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

# Get IP address for google
import socket
ai = socket.getaddrinfo("google.com", 80)
addr = ai[0][-1]

# Create a socket and make a HTTP request
s = socket.socket()
s.connect(addr)
s.send(b"GET / HTTP/1.0\r\n\r\n")

# Print the response
print(s.recv(1024))