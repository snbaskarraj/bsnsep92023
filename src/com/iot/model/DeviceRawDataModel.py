class Device_Raw_Data_Model:
    def __init__(self, device_id, timestamp, value, device_type):
        self._device_id = device_id
        self._timestamp = timestamp
        self._value = value
        self._device_type = device_type
