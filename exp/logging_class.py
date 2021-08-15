# importing DateTime package
from datetime import datetime


class AppLogger:
    """
    It is used save logs into a file
    :parameter
    ---- file: log file name Default is logfile.log
    """
    def __init__(self, file="logfile.log", mode="a", datetime_format="%d-%m-%Y %H:%M:%S"):
        self.f_name = file
        self.mode = mode
        self.datetime_format = datetime_format

    def log(self, log_type, log_msg):
        """
        Function log to save logs and log type in file
        :param log_type: Type of log-into, eror, warning, etc
        :param log_msg: Log to be saved (message)
        :return:
        """
        # current time
        now = datetime.now()
        # changing time format
        current_time = now.strftime(self.datetime_format)
        f = open(self.f_name, self.mode)
        f.write(current_time+"| "+log_type+"| "+log_msg+"\n")
        f.close()
        
