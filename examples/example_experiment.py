#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import time
import numpy as np
import pandas as pd
import datetime, pytz


# <span style="color:red">**Install the client library if not done so already**</span>.

# In[ ]:


# %pip install mke_clientlib


# <span style="color:red">**Import the client library**</span>

# In[ ]:


from mke_clientlib.rimlib import Experiment, get_utcnow


# ___
# # THIS IS A TEST NOTEBOOK TO PROTOTYPE THE EXECUTION CODE
# ___

# ## Default Parameters for this notebook

# In[ ]:


ticklen = 10

param1 = 5 # test paramaeter 1
param2 = 'A' # test paramaeter 1
param3 = ['A', 'B', 'C'] # test paramaeter 1


# ## Specific Parameters for this Script
# <span style="color:red">**The following cell will be written automatically by the processing bot when staring the notebook**</span>
# 
# Note, that the parameters written in the following cell also hold the meta information from the experiment this script is associated with.

# In[ ]:


# Parameters
dbserver_path='http://localhost:8080'
experiments_id = 1
devices = ['WTR']
antenna_id = "test_antenna"
script_in_path = "d:/repos/mke_eng/scripts/test_script.ipynb"
script_out_path = "d:/repos/mke_eng/tests/test_data/temp_out/test_antenna/20220701_1314_None_test_antenna_exp_script/20220701_1314_None_test_antenna_exp_script.ipynb"
script_version = "add9d522ecba2a5e930151d0667146f9_2022-07-01T08:43:33Z"
script_name = "test_script"
duration_expected_hr_dec = "0.1"
comments = ""
forecasted_oc = "N/A"
needs_manual_upload = False
param1 = 1


# In[ ]:


if isinstance(duration_expected_hr_dec, str):
    duration_expected_hr_dec = float(duration_expected_hr_dec)


# In[ ]:


my_dbobject = Experiment(experiments_id, dbserver_path)

# to test if correct
my_row_as_dict = my_dbobject.get_me()


# In[ ]:


my_row_as_dict = my_dbobject.set_status_running()


# In[ ]:


my_row_as_dict


# # THE ACTUAL TEST CODE GOES HERE
# ## Just Simulate Some Dummy Data and Wait

# In[ ]:


def make_some_dummy_data(t_len_sec, dt):
    channels = ['acu.actual_timestamp',
     'acu.general_management_and_controller.state',
     'acu.general_management_and_controller.p_act_az',
     'acu.general_management_and_controller.v_act_az',
     'acu.general_management_and_controller.p_point_corr_az',
     'acu.general_management_and_controller.p_act_el',
     'acu.general_management_and_controller.v_act_el',
     'acu.general_management_and_controller.p_point_corr_az',
    ]
    
    t_now = get_utcnow()
    dts = [(t_now + datetime.timedelta(seconds=dti)).timestamp() for dti in np.arange(0, t_len_sec, dt, dtype=float)]
    
    data = np.random.randint(1000, size=(len(dts), len(channels)))
    data[:,0] = dts
    return pd.DataFrame(data, columns=channels)

def make_some_dummy_auxdata(t_len_sec, dt):
    channels = ['time', 'wind_speed', 'wind_direction']
    
    t_now = get_utcnow()
    dts = [(t_now + datetime.timedelta(seconds=dti)).timestamp() for dti in np.arange(0, t_len_sec, dt, dtype=float)]
    
    data = np.random.randint(1000, size=(len(dts), len(channels)))
    data[:,0] = dts
    return pd.DataFrame(data, columns=channels)

def make_some_dummy_global_auxdata(t_len_sec, dt):
    channels = ['time', 'x_pixel', 'y_pixel', 'on_target']
    
    t_now = get_utcnow()
    dts = [(t_now + datetime.timedelta(seconds=dti)).timestamp() for dti in np.arange(0, t_len_sec, dt, dtype=float)]
    
    data = np.random.randint(1000, size=(len(dts), len(channels)))
    data[:,0] = dts
    return pd.DataFrame(data, columns=channels)


def testrun():          
    # simulate some test run...
    time.sleep(10)
    
    # simulate generating some data
    df_main = make_some_dummy_data(10, 1)
    dfs_dummy_aux = {k:make_some_dummy_auxdata(10, 1) for k in devices}
    
    # return it
    return df_main, dfs_dummy_aux
                             
def finish(t_elapsed):
    return {'OCS': make_some_dummy_global_auxdata(t_elapsed.total_seconds, 1)}, {'OCS': '.txt'}
    


# # RUN THE EXPERIMENT

# In[ ]:


t_start = get_utc_now()
print(make_zulustr(get_utcnow()) + ' | Starting...')

tickcount = 0
while tickcount < n_ticks_max: 
    tickcount += 1
    
    # ---------------------------------------
    # check if I should stop
    # ---------------------------------------
    
    if my_dbobject.get_remaining_time() <= ticklen*1.1:
        print(make_zulustr(get_utcnow()) + ' | Time is up... finshing')
        my_row_as_dict = my_dbobject.set_status_finishing()
        break
        
    if my_dbobject.check_for_cancel():
        print(make_zulustr(get_utcnow()) + ' | Cancle initiated from externally... cancelling')
        my_row_as_dict = my_dbobject.set_status_cancelling()
        break
        
    # get the start time...
    t_start_iter = get_utc_now()
    
    
    # ---------------------------------------
    # perform the test
    # ---------------------------------------
    df_main, dfs_dummy_aux = testrun()

    # ---------------------------------------
    # save the data
    # ---------------------------------------
    devices_to_add = {k:'.csv' for k in devices}
    mainfile_path, datafile_id, aux_data = my_dbobject.get_path_for_new_datafile(start_time=t_start_iter, devices_to_add=devices_to_add)
    
    print(f'Saving main data file with id {datafile_id} to: "{mainfile_path}"')
    df_main.to_csv(mainfile_path, index=False)
    
    for df_aux, (key, aux_id, aux_path) in zip(dfs_dummy_aux, aux_data):
        print(f'Saving aux data file with id {key, aux_id} to: "{aux_path}"')
        df_aux.to_csv(aux_path, index=False)
    
    print(make_zulustr(get_utcnow()) + f' | completed tick {tickcount} ...')
    
        
t_end = get_utc_now()
t_elapsed = t_start - t_end


# In[ ]:


# ---------------------------------------
# write any global data
# ---------------------------------------
if not my_row_as_dict['status'] == 'CANCELLING':
    dc_auxg_data, dc_auxg_ext = finish(t_elapsed)


    aux_data = my_dbobject.get_pathes_for_new_global_auxfiles(dc_auxg_ext)

    for (key_i, id_i, savepath_i) in aux_data:
        print(f'Saving global aux data file with id {key_i, id_i} to: "{savepath_i}"')
        dc_auxg_data[key_i].to_csv(aux_path, index=False)


# In[ ]:


my_row_as_dict

