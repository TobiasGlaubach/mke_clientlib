#!/usr/bin/python3
"""
MeerKAT Extension (MKE)
(r)emote (i)nterface (m)anagement (lib)rary
interface library for accessing remote experiment and analysis data in a dbserver
"""

import requests
import re
import datetime
import dateutil
import pytz
import os

allowed_status_codes = {

    'INITIALIZING': 0,
    'AWAITING_CHECK': 1,
    'WAITING_TO_RUN': 2,
    'HOLD': 3,
    
    'STARTING': 10,
    'RUNNING': 11,
    'CANCELLING': 12,
    'FINISHING': 13,

    'FINISHED': 100,
    'ABORTED': 101,

    'CANCELLED': 1001,
    'FAILED': 1000,
    'FAULTY': 1002,
}

def make_zulustr(dtobj:datetime.datetime, remove_ms = True) -> str:
    '''datetime.datetime object to ISO zulu style string
    will set tzinfo to utc
    will replace microseconds with 0 if remove_ms is given

    Args:
        dtobj (datetime.datetime): the datetime object to parse
        remove_ms (bool, optional): will replace microseconds with 0 if True . Defaults to True.

    Returns:
        str: zulu style string e.G. 
            if remove_ms: 
                "2022-06-09T10:05:21Z"
            else:
                "2022-06-09T10:05:21.123456Z"
    '''
    utc = dtobj.replace(tzinfo=pytz.utc)
    if remove_ms:
        utc = utc.replace(microsecond=0)
    return utc.isoformat().replace('+00:00','') + 'Z'

def parse_zulutime(s:str)->datetime.datetime:
    '''will parse a zulu style string to a datetime.datetime object. Allowed are
        "2022-06-09T10:05:21.123456Z"
        "2022-06-09T10:05:21Z" --> Microseconds set to zero
        "2022-06-09Z" --> Time set to "00:00:00.000000"
    '''
    try:
        if re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}Z', s) is not None:
            s = s[:-1] + 'T00:00:00Z'
        return dateutil.parser.isoparse(s).replace(tzinfo=pytz.utc)
    except Exception:
        return None



class RimObj():
    """base object to have the Analysis and Experiment 
    classes inherit from
    """
    def __init__(self, uri, tablename, id):
        self.uri = uri
        self.__tablename = tablename
        self.id = id

    @property
    def tablename(self):
        """this objects associated table name"""
        return self.__tablename

    def __get(self, tablename=None, id=None):
        if id is None:
            id = self.id
        if tablename is None:
            tablename = self.tablename
        r = requests.get(f'{self.uri}/{tablename}/{id}')
        assert r.status_code  == 200, r.text
        return r.json()

    def __patch(self, dc):
        r = requests.patch(f'{self.uri}/{self.tablename}/{self.id}', json=dc)
        assert r.status_code < 300, r.text
        return r.json()

    def get_me(self) -> dict:
        """returns the database entry row associated with this objects id as dictionary"""
        return self.__get()

    def get_my_antenna(self) -> dict:
        """returns the antenna entry row associated with this objects antenna_id as dictionary"""
        me = self.__get() 
        return self.__get('antennas', me['antenna_id'])

    def set_status(self, new_status:str, ignore_enum=False) -> dict:
        """set a new status to my object in the DB and return the updated 
        remote object as dictionary.
        The status must be one of:
            INITIALIZING, AWAITING_CHECK, WAITING_TO_RUN, HOLD, STARTING, 
            RUNNING, CANCELLING, FINISHING, FINISHED, ABORTED, CANCELLED, 
            FAILED, FAULTY
        but ideally you should only set RUNNING, or FINISHING. See 
            .set_status_finishing()
            .set_status_running()

        Args:
            new_status (str): the new status to set. See this library
            ignore_enum (bool, optional): set True to not check if the new status is allowed. Defaults to False.

        Returns:
            dict: the database entry row associated with this objects id as dictionary
        """
        assert new_status in allowed_status_codes or ignore_enum, "the given status was not within the allowed status strings: allowed are: " + ', '.join(allowed_status_codes.keys())
        return self.__patch(self.id, dict(status=new_status))

    def set_status_finishing(self) -> dict:
        """set FINISHING as new status to my object in the DB and return the updated 
        remote object as dictionary.

        Returns:
            dict: the database entry row associated with this objects id as dictionary
        """
        return self.set_status('FINISHING')

    def set_status_running(self) -> dict:
        """set RUNNING as new status to my object in the DB and return the updated 
        remote object as dictionary.

        Returns:
            dict: the database entry row associated with this objects id as dictionary
        """
        return self.set_status('RUNNING')

    def check_for_cancel(self) -> bool:
        """gets the remote table row associated with me and returns whether or not a cancel was requested
        
        Returns:
            bool: true if cancel was requested, false if not
        """
        me = self.__get()
        assert me['status'] in allowed_status_codes, 'ERROR! The remote status ' + me['status'] + ' is unrecognized'
        return allowed_status_codes[me['status']] >= 100 or me['status'] == 'CANCELLING'

    
    def get_remaining_time(self) -> float:
        """gets my remote object and checks how much time it 
        is allowed to be running by returning
            (start_condition + duration_expected) < utcnow

        Returns:
            float: the remaining time in hours for this script to run
        """
        me = self.__get()
        tstart = parse_zulutime(me['start_condition'])
        assert tstart is not None, '"start_condition" could not be parsed. Got: {} {}'.format(type(me['start_condition']), me['start_condition'])
        t_is = datetime.datetime.utcnow()
        t_end_req = tstart + datetime.timedelta(hours=me['duration_expected_hr_dec'])
        t_rem = (t_end_req - t_is).total_seconds() / 60.0 / 60.0
        return max(0, t_rem)


class Experiment(RimObj):
    """An interface object to get access to experiments in the 
    database.

    Args:
        RimObj: _description_
    """
    __tablename = 'experiments'
    def __init__(self, id, uri = None):
        """create a new Experiment object with an id to get access 
        to this expiriment objects row in the database

        Args:
            id (int): the id of the analyses in the DB
            uri (string, optional): the URI to connect to. 
                If not given will be tried to be resolved 
                from environmental valiables.
                    Defaults to None.
        """
        if uri is None:
            uri = os.environ.get('DBSERVER_URI')
        assert uri, 'need to give a valid URI for a DB connection!'
        super().__init__(uri, self.__tablename, id)


class Analysis(RimObj):
    """An interface object to get access to analyses in the 
    database."""
    __tablename = 'analyses'
    def __init__(self, id, uri = None):
        """create a new Analysis object with an id to get access 
        to this analyses objects row in the database

        Args:
            id (int): the id of the analyses in the DB
            uri (string, optional): the URI to connect to. 
                If not given will be tried to be resolved 
                from environmental valiables.
                    Defaults to None.
        """
        if uri is None:
            uri = os.environ.get('DBSERVER_URI')
        assert uri, 'need to give a valid URI for a DB connection!'
        super().__init__(uri, self.__tablename, id)

