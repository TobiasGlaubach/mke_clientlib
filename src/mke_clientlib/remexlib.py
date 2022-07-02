#!/usr/bin/python3
"""
(rem)mote (ex)ecution (lib)rary
interface library for remote scheduling of scripts on procserver and accessing data entries in dbserver
"""

import requests
import json
import re
import datetime, dateutil, pytz
import numpy as np
import pandas as pd

status_dc = {
    'INITIALIZING': 0,
    'AWAITING_CHECK': 1,
    'WAITING_TO_RUN': 2, 
    
    "STARTING": 10, 
    "RUNNING": 11, 
    "CANCELLING": 12, 
    "STOPPING": 13, 

    "AWAITING_POST_PROCESSING": 100, 
    "FINISHED": 101, 
    "ABORTED": 102,
    "CANCELLED": 1001,
    "FAILED": 1000, 
    "FAULTY": 1002,
    "POST_PROC_FAILED": 1003
}

verbose = 0



script_fields = {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",

        "caldav_uid": "TEXT",
        "caldav_calendar": "TEXT",

        "script_name": "TEXT",
        "script_version": "TEXT",
        "script_in_path": "TEXT",
        "script_out_path": "TEXT",

        "antenna_id": "TEXT",
        "script_params_json": "TEXT",
        
        "results_json": "TEXT",

        "start_condition": "TEXT",
        "time_initiated_iso": "TEXT",
        "duration_expected_hr_dec": "REAL",
        "time_started_iso": "TEXT",
        "time_finished_iso": "TEXT",

        "status": "TEXT",
        "errors": "TEXT",

        "devices_json": "TEXT",
        "needs_manual_upload": "INTEGER",
        "comments": "TEXT",
        "forecasted_oc": "TEXT",
        "papermill_json": "TEXT",
        "aux_files_json": "TEXT",
        
        "last_change_time_iso":'TEXT', 
        "change_history": 'TEXT'
    }

def __get_starttime(row, i, df):
    if 'time_started_iso' in row and row['time_started_iso']:
        return row['time_started_iso']
    elif parse_zulutime(row['start_condition']):
        return parse_zulutime(row['start_condition'])


def __get_endtime(row, i, df):
    if 'time_finished_iso' in row and row['time_finished_iso']:
        return row['time_finished_iso']
    elif row['tstart'] is not None and row['duration_expected_hr_dec'] is not None:
        return row['tstart'] + datetime.timedelta(hours=row['duration_expected_hr_dec'])
    else:
        return row['tstart'] + datetime.timedelta(minutes=1)

################################################################################
################################################################################
################################################################################


def make_zulustr(dtobj, remove_ms = True):
    '''helper function to make a zulu style iso datetime format string like
    2022-06-29T00:24:12Z for remove_ms=True
    2022-06-29T00:24:12.123456Z for remove_ms=False'''
    utc = dtobj.replace(tzinfo=pytz.utc)
    if remove_ms:
        utc = utc.replace(microsecond=0)
    return utc.isoformat().replace('+00:00','') + 'Z'

def get_utcnow():
    """gets current utc time timezone aware as datetime.datetime object from local clock
    """
    return datetime.datetime.utcnow().replace(tzinfo=pytz.utc)


def parse_zulutime(s):
    """tries parsing any iso style format datetime string and returns it as timezone aware datetime.datetime object. 
    
    will return None on fail"""
    try:
        if re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}Z', s) is not None:
            s = s[:-1] + 'T00:00:00Z'
        return dateutil.parser.isoparse(s).replace(tzinfo=pytz.utc)
    except Exception:
        return None



################################################################################
################################################################################
################################################################################

def add_antenna(uri, id, address, altitude=1086, lat=-30.717972, lon=21.413028, comments='', params_json='', software_version='000', configuration=None):
    """Convenience Function to add an antenna to the remote database based on arguments

    Args:
        uri (str): the remote database server api link
        id (str): the id (name) to give this antenna
        address (str): the http(s) address under which to reach the antenna 
        altitude (int, optional): The altitute in meter for this antenna. Defaults to 1086.
        lat (float, optional): the latitude for this antenna. Defaults to -30.717972.
        lon (float, optional): the longitude for this antenna. Defaults to 21.413028.
        comments (str, optional): any string comments to add in the DB. Defaults to ''.
        params_json (str, optional): the antenna parameters as json encoded string ip applicable. Defaults to ''.
        software_version (str, optional): The antenna software version currently running. Defaults to '000'.
        configuration (str, optional): any string you want to give for configuration info. Defaults to None.

    Returns:
        dict: the dictionary returned by the remote api for the generated antenna if successful otherwise raises an error
    """
    dc = dict(id=id, 
        address=address, 
        software_version=software_version, 
        configuration=configuration, 
        altitude=altitude, 
        lat=lat, 
        lon=lon, 
        params_json=params_json, comments=comments)

    r = requests.post(f'{uri}/antennas', json=dc)
    assert r.status_code < 300, r.text
    return r.json()

    
    

################################################################################
################################################################################
################################################################################

class ScriptMinimal():
    """Minimal Script object. This object can be used to register scripts on a server

    Args:
        script_in_path (str, optional): the path for the input script file. Defaults to None.
        antenna_id (str, optional): the antenna if which to use for this script. Defaults to None.
        start_condition (str, optional): the start condition (zulutime as string) to use. None for NOW!. Defaults to None.
        duration_expected_hr_dec (float, optional): The expected duration for this script to run in hours with decimal fractions. Defaults to 0.1.
        status (str, optional): the status to set on creation. Defaults to 'INITIALIZING'.
        devices_json (str, optional): A list of devices used while running this experiment. Defaults to '[]'.
        script_params_json (str, optional): json encoded dictionary holding the additional parameters for this script. Defaults to '{}'.
        needs_manual_upload (int, optional): set to 1 in order to block post processing until data has been uploaded manually. Defaults to 0.
    """
    def __init__(self, script_in_path:str=None, antenna_id:str=None, 
                    start_condition=None, 
                    duration_expected_hr_dec:float=0.1, 
                    status='INITIALIZING', 
                    devices_json='[]', 
                    script_params_json = '{}',
                    needs_manual_upload:int=0, 
                    **kwargs) -> None:


        if start_condition is None:
            start_condition = make_zulustr(get_utcnow())

        assert script_in_path, '"script_in_path" must be given'
        assert antenna_id, '"antenna_id" must be given'
        assert start_condition, '"start_condition" must be given'
        assert duration_expected_hr_dec >= 0, '"duration_expected_hr_dec" can not be negative'
        assert needs_manual_upload == 0 or needs_manual_upload== 1, '"needs_manual_upload" needs to be 1|0'
        assert status in status_dc, '"status" must be in ' + ', '.join(status_dc.keys())
        
        if isinstance(devices_json, list):
            devices_json = json.dumps(devices_json)
        else:
            devices = json.loads(devices_json) # test json parse

        if isinstance(script_params_json, dict):
            script_params_json = json.dumps(script_params_json)
        else:
            script_params = json.loads(script_params_json) # test json parse

        self.script_in_path = script_in_path
        self.antenna_id = antenna_id
        self.start_condition = start_condition
        self.script_in_path = script_in_path
        self.duration_expected_hr_dec = duration_expected_hr_dec
        self.needs_manual_upload = needs_manual_upload
        self.devices_json = devices_json
        self.script_params_json = script_params_json

        for k, v in kwargs:
            assert k in script_fields, f'"{k}" is not in the allowed fields for a script object. Allowed are: "' + ','.join(script_fields.keys()) + '"'
            setattr(self, k, v)

    @property
    def start_time(self):
        """Get the starttime as timezone aware datetime.datetime object (utc!)"""
        return parse_zulutime(self.start_condition)

    @property
    def end_time(self):
        """Get the estimated end time from start time and duration as timezone aware datetime.datetime object (utc!)"""
        return self.start_time + datetime.timedelta(hours=self.duration_expected_hr_dec)

    def to_dict(self):
        """get a dict which can be passed to the 'experiments' or 'analyses' api"""
        return {k:getattr(self, k) for k in script_fields.keys() if hasattr(self, k)}

################################################################################
################################################################################
################################################################################

class RemexApiAccessor():
    def __init__(self, uri, scriptype):
        self.uri = uri
        self.scriptype = scriptype

    def post(self, dc):
        r = requests.post(f'{self.uri}/{self.scriptype}', json=dc)
        assert r.status_code < 300, r.text
        return r.json()

    def get(self, id, scriptype=None):
        scriptype = scriptype if scriptype else self.scriptype
        r = requests.get(f'{self.uri}/{scriptype}/{id}')
        assert r.status_code  == 200, r.text
        return r.json()

    def patch(self, id, dc):
        r = requests.patch(f'{self.uri}/{self.scriptype}/{id}', json=dc)
        assert r.status_code < 300, r.text
        return r.json()

    def set_status(self, id, new_status, ignore_enum=False):
        assert new_status in status_dc or ignore_enum, "the given status was not within the allowed status strings: allowed are: " + ', '.join(status_dc.keys())
        return self.patch(id, dict(status=new_status))


    def get_minimal_example(self, script_in_path:str=None, antenna_id:str=None, 
                    start_condition=None, 
                    duration_expected_hr_dec:float=0.1, 
                    status='INITIALIZING', 
                    devices_json='[]', 
                    script_params_json = '{}',
                    needs_manual_upload:int=0, 
                    **kwargs):
                    
        script = ScriptMinimal(script_in_path, 
                    antenna_id, 
                    start_condition, 
                    duration_expected_hr_dec, 
                    status, 
                    devices_json, 
                    script_params_json,
                    needs_manual_upload, 
                    **kwargs)
        dc = script.get_dict()
        return dc


    def get_full_example(self, script_in_path, antenna_id='test_antenna',  start_condition = None, script_params=None):
        
        if not start_condition:
            start_condition =  make_zulustr(datetime.datetime.utcnow())
        
        r = requests.get(f'{self.uri}/get_schemas')
        assert r.status_code  == 200, r.text
        dc = r.json()

        dc['antenna_id'] = antenna_id,
        dc['script_in_path'] = script_in_path,
        dc['start_condition'] = start_condition,

        if not script_params:
            dc['script_params_json'] = json.dumps(script_params)
        return dc


    def pre_check(self, dc):
        lnk = f'{self.uri}/pre_check/{self.scriptype}'
        if verbose > 0:
            print(lnk)
        if verbose > 1:
            print(json.dumps(dc, indent=2))

        r = requests.post(lnk, json=dc)
        assert r.status_code < 300, r.text
        dcr = r.json()
        assert dcr['msg'] is None, dcr['msg']
        return dcr['data'], dcr['status']


    def check_and_start(self, dc):
        lnk = f'{self.uri}/check_and_start/{self.scriptype}'
        if verbose > 0:
            print(lnk)
        r = requests.post(lnk, json=dc)
        assert r.status_code < 300, r.text
        dcr = r.json()
        assert dcr['msg'] is None, dcr['msg']
        return dcr['data'], dcr['status']


    def abort(self, id):
        return requests.get(f'{self.uri}/abort/{self.scriptype}/{id}')

    def cancel(self, id):
        return requests.get(f'{self.uri}/cancel/{self.scriptype}/{id}')

    def set_chained_start(self, id_after, id_pre, pre_type=None, delay_minutes=0):

        other = self.get(self, id_after, pre_type)
        dur = float(other['duration_expected_hr_dec'])
        new_time = parse_zulutime(other['start_condition']) + datetime.timedelta(hours=dur) + datetime.timedelta(minutes=delay_minutes)

        tsrt = make_zulustr(new_time)

        self.reschedule(id_pre, new_start_time=tsrt)
    
    def set_timed_start(self, id, starttime=None):
        if starttime is None: 
            starttime = make_zulustr(datetime.datetime.utcnow())
        self.reschedule(id, new_start_time=starttime)

    def reschedule(self, id, new_start_time=None, new_duration=None, new_end_time=None):
        tmp = [new_start_time, new_duration, new_end_time]
        tmp_c = [c is not None for c in tmp]

        assert any(tmp_c), "either starttime, endtime, or pre_id must be given"

        obj = self.get(id)
        to_updt = {}
        sc = obj['start_condition'].strip()

        
        if new_duration is not None:
            assert new_end_time is None, 'either duration or end_time can be updated. Not both'
            to_updt['duration_expected_hr_dec'] = float(new_duration)
        
        if new_start_time is not None:
            to_updt['start_condition'] = new_start_time

        if new_end_time is not None:
            assert new_duration is None, 'either duration or end_time can be updated. Not both'
            ts = parse_zulutime(sc)
            assert ts, "must have proper start time string. Got: " + sc
            
            te = parse_zulutime(new_end_time)
            assert ts, "must have proper end time string. Got: " + new_end_time

            dt = (te-ts).total_seconds() / 60.0 / 60.0
            to_updt['duration_expected_hr_dec'] = dt

        return self.patch(id, to_updt)
        

################################################################################
################################################################################
################################################################################

def add_script(uri, script_type:str, script_in_path:str=None, antenna_id:str=None, 
                    start_condition=None, 
                    duration_expected_hr_dec:float=0.1, 
                    status='INITIALIZING', 
                    devices_json='[]', 
                    script_params_json = '{}',
                    needs_manual_upload:int=0, 
                    **kwargs):
    """convenience function 

    Args:
        script_in_path (str, optional): the path for the input script file. Defaults to None.
        antenna_id (str, optional): the antenna if which to use for this script. Defaults to None.
        start_condition (str, optional): the start condition (zulutime as string) to use. None for NOW!. Defaults to None.
        duration_expected_hr_dec (float, optional): The expected duration for this script to run in hours with decimal fractions. Defaults to 0.1.
        status (str, optional): the status to set on creation. Defaults to 'INITIALIZING'.
        devices_json (str, optional): A list of devices used while running this experiment. Defaults to '[]'.
        script_params_json (str, optional): json encoded dictionary holding the additional parameters for this script. Defaults to '{}'.
        needs_manual_upload (int, optional): set to 1 in order to block post processing until data has been uploaded manually. Defaults to 0.
    """

    api = RemexApiAccessor(uri, scriptype=script_type)
    script = ScriptMinimal(script_in_path, 
                    antenna_id, 
                    start_condition, 
                    duration_expected_hr_dec, 
                    status, 
                    devices_json, 
                    script_params_json,
                    needs_manual_upload, 
                    **kwargs)
    

    return api.check_and_start(script.to_dict())


def test_script(uri, script_type:str, script_in_path:str=None, antenna_id:str=None, 
                    start_condition=None, 
                    duration_expected_hr_dec:float=0.1, 
                    status='INITIALIZING', 
                    devices_json='[]', 
                    script_params_json = '{}',
                    needs_manual_upload:int=0, 
                    **kwargs):

    api = RemexApiAccessor(uri, scriptype=script_type)
    script = ScriptMinimal(script_in_path, 
                    antenna_id, 
                    start_condition, 
                    duration_expected_hr_dec, 
                    status, 
                    devices_json, 
                    script_params_json,
                    needs_manual_upload, 
                    **kwargs)
    

    return api.pre_check(script.to_dict())
    

################################################################################
################################################################################
################################################################################


def raise_on_overlap(df_in):
    for antenna_id in df_in['antenna_id'].unique():
        df = df_in[df_in['antenna_id'] == antenna_id]
        df = df.sort_values('tstart')
        if raise_on_overlap:
            ol = df['tend'].iloc[:-1].values - df['tstart'].iloc[1:].values
            overlap = df['tend'].iloc[:-1].values >= df['tstart'].iloc[1:].values
            assert not np.any(overlap), 'Found overlap at: ' + str(overlap) + str(df[['tstart', 'tend']]) + str(ol)


def get_expected_schedule(df):
    df['tstart'] = pd.Series({i:__get_starttime(row, i, df) for i, row in df.iterrows()})
    df['tend'] = pd.Series({i:(__get_endtime(row, i, df) if not pd.isnull(row['tstart']) else None) for i, row in df.iterrows()})
    return df


