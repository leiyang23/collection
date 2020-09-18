# -*- coding:utf-8  -*-
"""
time: 2020-09-18 23:51
"""
import ctypes
import os
import platform
import sched
import time

MAX_ALIVE_DAYS = 30
SAFE_DAYS = 10
SAFE_FREE_SPACE = 1024 * 1024 * 1024 * 3


class Sweeper:
    """file sweeper

    clear the expired files under the specified path.
    """

    def __init__(self, base_path, safe_age=None, max_age=None, file_func=None, folder_func=None, by_modify=True):
        """
        :param base_path: root directory
        :param safe_age: minimum seconds for a file to survive
        :param max_age: if seconds of existence exceed this value, file will be handled
        :param file_func: when file is expired, this func will be called
        :param folder_func: the func to deal with empty folder
        :param by_modify:  which is time of last modification or last access
        """
        self.base_path = base_path
        self.by_modify = by_modify

        self.safe_age = safe_age or 3600 * 24 * SAFE_DAYS
        self.max_age = max_age or 3600 * 24 * MAX_ALIVE_DAYS

        self.file_func = file_func or self.remove
        self.folder_func = folder_func or self.remove

    def scan(self):
        for root, dirnames, filenames in os.walk(self.base_path, topdown=False):
            for filename in filenames:
                filepath = os.path.join(root, filename)

                file_stat = os.stat(filepath)
                basis_time = file_stat.st_mtime if self.by_modify else file_stat.st_atime
                if time.time() - basis_time > self.max_age:
                    self.file_func(filepath)

            for dirname in dirnames:
                dirpath = os.path.join(root, dirname)
                if not os.listdir(dirpath):
                    self.folder_func(dirpath)

    def remove(self, path):
        if os.path.isdir(path):
            os.rmdir(path)

        if os.path.isfile(path):
            os.remove(path)


class Policy:
    """run policy

    two ways to run:
    - single
    - cycle
    """

    def __init__(self, safe_space=None, sweeper=None, alarm_func=None):

        self.safe_space = safe_space or SAFE_FREE_SPACE

        if not sweeper:
            raise ValueError("not supply Sweeper Object!")
        self.sweeper = sweeper
        self._base_path = self.sweeper.base_path

        self.alarm_func = alarm_func

    def run_single(self):
        self._do()

    def run_cycle(self, unit="d", num=1, start_now=True):
        units = {
            "d": 3600 * 24,
            "day": 3600 * 24,
            "w": 3600 * 24 * 7,
            "week": 3600 * 24 * 7,
            "m": 3600 * 24 * 30,
            "month": 3600 * 24 * 30,
        }
        if unit.lower() not in units:
            raise ValueError("not support unit! Optional units: 'd'(day) 'w'(week) 'm'(month).")

        interval = units[unit.lower()] * num

        scheduler = sched.scheduler(time.time, time.sleep)
        scheduler.enter(3 if start_now else interval, 0, self._delay, argument=(scheduler, interval))
        scheduler.run()

    def _delay(self, scheduler, interval):
        scheduler.enter(interval, 0, self._delay, argument=(scheduler, interval))
        self._do()

    def _do(self):
        """Keep files as long as possible on the basis of remaining disk space
        """
        cur_free = 0
        retain_days = MAX_ALIVE_DAYS

        while cur_free < self.safe_space and retain_days > SAFE_DAYS:
            self.sweeper.scan()
            cur_free = self.get_free_space(self._base_path)

            if retain_days <= SAFE_DAYS:
                if callable(self.alarm_func):
                    self.alarm_func(self._base_path, cur_free=cur_free)

            retain_days -= 0.5

    @staticmethod
    def get_free_space(folder):
        """ Return folder free space (in bytes)
        """
        if platform.system() == 'Windows':
            free_bytes = ctypes.c_ulonglong(0)
            ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(folder), None, None, ctypes.pointer(free_bytes))
            return free_bytes.value
        else:
            st = os.statvfs(folder)
            return st.f_bavail * st.f_frsize


if __name__ == '__main__':
    Policy(sweeper=Sweeper(os.path.join(os.path.expanduser('~'), "Downloads"))).run_single()
