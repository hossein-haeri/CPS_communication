import numpy as np
import socket
import time
import random
import threading
import sys
import pandas as pd

# class Client:
#     def __init__(self, vehicle_id, vehicle_ip):
#         self.id = vehicle_id
#         self.ip = vehicle_ip


class Server:
    def __init__(self, ip):
        self.ip = ip
        self.Q = []
        self.clients = pd.DataFrame({
                                     'id':[],
                                     'ip': [],
                                     'num_meas':[],
                                     'curr_road_id':[],
                                     'curr_lane_id':[]
                                     })
        self.clients = self.clients.set_index('id') # make the 'id' the indexing column for easier access

    def is_registered(self, vehicle_id):
        if vehicle_id in set(self.clients['id']):
            return True
        return False

    
    def register_vehicle(self, vehicle_id, vehicle_ip):
        self.clients = self.clients.append({
                                             'id': str(vehicle_id),
                                             'ip': str(vehicle_ip),
                                             'num_meas': 0,
                                             'curr_road_id':None,
                                             'curr_lane_id':None,
                                             'curr_s':None
                                             })



    def dump_measurement(self,row):
        pass


    def get_measurements(self,sock,port):
        try:
            while True:
                msg, addr = sock.recvfrom(2048) # buffer size is 2048tes
                row = msg.decode().split(', ')
                vehicle_id = row[0]
                vehicle_ip = addr[0]

                # self.clients.loc[vehicle_id]['num_meas'] += 1
                # dump the measurement into the databse
                self.dump_measurement(row)



                print("received a measurement from: vechile ID: %s, vehicle IP: %s" % (vehicle_id, vehicle_ip))
        except KeyboardInterrupt:
            print('interrupted!')


    def get_requests(self,sock,port):
        try:
            while True:
                msg, addr = sock.recvfrom(2048) # buffer size is 1024 bytes
                row = msg.decode().split(', ')
                vehicle_id = row[0]
                vehicle_ip = addr[0]
                self.Q.append(row)
                print("received a preview request from: vechile ID: %s, vehicle IP: %s" % (vehicle_id, vehicle_ip))
        except KeyboardInterrupt:
            print('interrupted!')


    def respond(self,sock,PORT):
        try:    
            while True:
                if len(self.Q) > 0:
                    num_segments = 10
                    fric = np.random.rand(num_segments)*2
                    s_from = np.random.uniform(.1,.9,num_segments)
                    d = np.random.uniform(0.01,.05,num_segments)
                    fric_str_list = [str(element) for element in fric]
                    fric_joined_string = ",".join(fric_str_list)
                    resp = bytes(fric_joined_string,"utf-8")
                    # print("response is sent: %s" % resp)
                    IP = "127.0.0.1" # NEEDS TO BE CHANGED TO VEHICLE ID
                    sock.sendto(resp, (IP, PORT))
                    self.Q.pop(0)
                    print(len(self.Q))
        except KeyboardInterrupt:
            print('interrupted!')



if __name__ == '__main__':
    

    SERVER_IP = "127.0.0.1"
    MEAS_PORT = 5001
    PREV_PORT = 5002

    server = Server(SERVER_IP)

    sock_preview = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_preview.bind((SERVER_IP, PREV_PORT))
    sock_measurement = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_measurement.bind((SERVER_IP, MEAS_PORT))

    # t_resp = threading.Thread(target=send_response,args=(sock, SERVER_IP, RES_PORT))
    # t_resp.start()

    t_get_meas = threading.Thread(target=server.get_measurements,args=(sock_measurement,MEAS_PORT))
    t_get_req = threading.Thread(target=server.get_requests,args=(sock_preview,PREV_PORT))
    t_resp = threading.Thread(target=server.respond,args=(sock_preview,PREV_PORT))

    t_get_meas.start()
    t_get_req.start()
    t_resp.start()

    # server.get_measurement(sock_measurement)
    # server.get_request(sock_preview)