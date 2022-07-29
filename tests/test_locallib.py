
import datetime
import unittest
import astropy.units as u
from astropy.time import Time
import numpy as np
import pandas as pd

import matplotlib.pyplot as plt


import os, inspect, sys
# path was needed for local testing
current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
print(parent_dir)
sys.path.insert(0, parent_dir + '/src')
print(sys.path)


from mke_client.helpers import get_utcnow, make_zulustr
from mke_client.locallib import LocalExperiment


class TestLocalExperiment(unittest.TestCase):

    def test_all_simple(self):
        devices_to_add = ['ACU', 'TST']
        dc_auxg_ext = {'MWS': '.zip', 'STS': '.csv'}

        

        my_dbobject = LocalExperiment(0)


        time = my_dbobject.get_remaining_time(get_utcnow())
        self.assertGreater(time, 0)
        my_dbobject.set_status_finishing()
        me = my_dbobject.get()
        self.assertEqual('FINISHING', me['status'])
        my_dbobject.set_status_cancelling()
        me = my_dbobject.get()
        self.assertEqual('CANCELLING', me['status'])

        t_start_iter = get_utcnow()

        mainfile_path, datafile_id, aux_data = my_dbobject.get_path_for_new_datafile(start_time=t_start_iter, devices_to_add=devices_to_add)
        
        mainfile_path, datafile_id, aux_data = my_dbobject.get_path_for_new_datafile(start_time=t_start_iter, devices_to_add=devices_to_add)
        aux_data = my_dbobject.get_pathes_for_new_global_auxfiles(dc_auxg_ext)

        self.assertTrue(aux_data)
        self.assertTrue(mainfile_path)
        self.assertTrue(datafile_id)

    def test_wait_start_condition(self):

        my_dbobject = LocalExperiment(0)
        my_dbobject.wait_for_start_condition()

        import time
        dtwait = 5
        tstart = time.time()
        my_dbobject = LocalExperiment(0, start_condition=make_zulustr(get_utcnow() + datetime.timedelta(seconds=dtwait)) )
        my_dbobject.wait_for_start_condition()
        dt = time.time() - tstart

        self.assertGreater(dt, dtwait)

if __name__ == "__main__":
    sim = TestLocalExperiment()
    sim.test_wait_start_condition()

    unittest.main()
