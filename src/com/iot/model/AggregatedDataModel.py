import datetime
import uuid
from decimal import Decimal


class Aggregated_Data_Model:
    # device_type: str
    # average: str
    # minimum: str
    # maximum: str
    # start_time: str
    # end_time: str
    # deviceid: str
    # id: str
    # timestamp: str

    def __init__(self, deviceid, device_type, avg, minm, maxm, start_time, end_time):
        self.device_type = device_type
        self.average = Decimal(avg)
        self.minimum = Decimal(minm)
        self.maximum = Decimal(maxm)
        self.start_time = str(start_time)
        self.end_time = str(end_time)
        self.deviceid = deviceid
        self.id = str(uuid.uuid4())
        self.timestamp = str(datetime.datetime.now())

    def __repr__(self):
        return "(" \
               "Id = {0}, " \
               "DeviceId = {1}, " \
               "Sensor={2}, " \
               "AverageValue= {3}, " \
               "Minimum= {4}," \
               "Maximum= {5}," \
               "start_time = {6}, " \
               "end_time = {7} ) \n".format(self.deviceid, self.deviceid, self.device_type, self.average, self.minimum,
                                            self.maximum, self.start_time, self.end_time)

    # def marshall_data(self):
    #     data = {}
    #     for variable, value in vars(self).items():
    #         data[variable] = value
    #
    #     # print("data ",data)
    #     return data
