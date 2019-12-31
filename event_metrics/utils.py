import time


def _current_time_us():
    return int(time.time() * 1e6)
