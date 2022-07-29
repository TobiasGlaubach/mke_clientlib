
import datetime
import dateutil
import pytz

import re

def get_utcnow():
    """get current UTC date and time as datetime.datetime object timezone aware"""
    return datetime.datetime.utcnow().replace(tzinfo=pytz.utc)

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

