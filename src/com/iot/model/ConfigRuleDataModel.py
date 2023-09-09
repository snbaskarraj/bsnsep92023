class Config_Rule_Data_Model:
    def __init__(self, ruleid, rule_type,device_type, avg_min, avg_max, trigger_count):
        self.id = ruleid
        self.rule_type = rule_type
        self.device_type = device_type
        self.avg_min = avg_min
        self.avg_max = avg_max
        self.trigger_count = trigger_count

    def __repr__(self):
        return "(" \
               "id = {0}, " \
               "rule_type = {1}, " \
               "device_type = {2}, " \
               "avg_min = {3}, " \
               "avg_max={4}, " \
               "trigger_count= {5} )".format( self.id, self.rule_type, self.device_type, self.avg_min, self.avg_max, self.trigger_count)
