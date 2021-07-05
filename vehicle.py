import numpy as np
import socket
import time
import random
import threading
import sys








# def send_measurement():
#   with open("data.csv") as f:
#       lis = [line.split() for line in f]        # create a list of lists
#       print(lis[0])
#       for i, x in enumerate(lis):              #print the list items 
#           MESSAGE = bytes(str(VEHICLE_ID)+', '+x[0],"utf-8")
#           print("measurement is sent: %s" % MESSAGE)
#           print("\n")
#           sock.sendto(MESSAGE, (UDP_IP, UDP_PORT))
#           time.sleep(1) # wait for 1 second


class Vehicle:
    def __init__(self, ip):
        self.id = random.randrange(1000)
        self.ip = ip
        self.road_id = random.randrange(10000)
        self.lane_id = random.randrange(5)      
        self.s = np.random.rand()

    def send_measurement(self,sock,IP,PORT):
        try:    
            while True:
                fric = np.random.rand()*2
                msg = bytes(str(self.id)+', '+str(self.road_id)+', '+str(self.lane_id)+', '+str(self.s)+', '+str(fric),"utf-8")
                print("measurement is sent: %s" % msg.decode())
                sock.sendto(msg, (IP, PORT))
                time.sleep(0.25)
        except KeyboardInterrupt:
            print('interrupted!')

    def request_preview(self,sock,IP,PORT):
        d = 0.002
        try:
            while True:
                msg = bytes(str(self.id)+', '+str(self.road_id)+', '+str(self.s)+', '+str(self.s + d),"utf-8")
                sock.sendto(msg, (IP, PORT))
                print("preview request has been sent: %s" % msg.decode())
                time.sleep(2.0)
        except KeyboardInterrupt:
            print('interrupted!')

    def get_response(self,sock,IP,PORT):
        try:
            while True:
                resp, addr = sock.recvfrom(2048) # buffer size is 2048 bytes
                print("received database response: %s" % resp.decode())
        except KeyboardInterrupt:
            print('interrupted!')


if __name__ == '__main__':

    SERVER_IP = "127.0.0.1"
    VEHICLE_IP = "127.0.0.1"
    MEAS_PORT = 5001
    PREV_REQ_PORT = 5002
    PREV_RES_PORT = 5003

    sock_measurement = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_preview_request = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_preview_response = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_preview_response.bind((VEHICLE_IP, PREV_RES_PORT))


    vehicle = Vehicle(VEHICLE_IP)



    # # move the vehicle
    # vehicle.s += 0.0000001
    # if vehicle.s >= 1:
    #     vehicle = 0.0
            
    t_meas = threading.Thread(target=vehicle.send_measurement,args=(sock_measurement, SERVER_IP, MEAS_PORT))
    t_prev_req = threading.Thread(target=vehicle.request_preview,args=(sock_preview_request, SERVER_IP, PREV_REQ_PORT))
    t_prev_res = threading.Thread(target=vehicle.get_response, args=(sock_preview_response, SERVER_IP, PREV_RES_PORT))


    t_meas.start()
    t_prev_req.start()
    t_prev_res.start()


