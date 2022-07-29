
#!/usr/bin/python3
""" 
generates the default file pathes etc
"""


# import datetime
import os

import re
import dateutil.parser, datetime, time, pytz

import logging
import sys

_log = logging.getLogger()
streamHandler = logging.StreamHandler(sys.stdout)
streamHandler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
_log.addHandler(streamHandler)




from pathlib import Path

import tempfile
from pathlib import Path
home = str(Path.home())

# if __name__ == '___main__':
#     import sys, inspect, os
#     # path was needed for local testing
#     current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
#     parent_dir = os.path.dirname(current_dir)
#     sys.path.insert(0, parent_dir)


################################################################################################
################################################################################################
################################################################################################

# from https://stackoverflow.com/questions/9532499/check-whether-a-path-is-valid-in-python-without-creating-a-file-at-the-paths-ta

import errno, os, sys

# Sadly, Python fails to provide the following magic number for us.
ERROR_INVALID_NAME = 123
'''
Windows-specific error code indicating an invalid pathname.

See Also
----------
https://docs.microsoft.com/en-us/windows/win32/debug/system-error-codes--0-499-
    Official listing of all such codes.
'''

def is_pathname_valid(pathname: str) -> bool:
    '''
    `True` if the passed pathname is a valid pathname for the current OS;
    `False` otherwise.
    '''
    # If this pathname is either not a string or is but is empty, this pathname
    # is invalid.
    try:
        if not isinstance(pathname, str) or not pathname:
            return False

        # Strip this pathname's Windows-specific drive specifier (e.g., `C:\`)
        # if any. Since Windows prohibits path components from containing `:`
        # characters, failing to strip this `:`-suffixed prefix would
        # erroneously invalidate all valid absolute Windows pathnames.
        _, pathname = os.path.splitdrive(pathname)

        # Directory guaranteed to exist. If the current OS is Windows, this is
        # the drive to which Windows was installed (e.g., the "%HOMEDRIVE%"
        # environment variable); else, the typical root directory.
        root_dirname = os.environ.get('HOMEDRIVE', 'C:') \
            if sys.platform == 'win32' else os.path.sep
        assert os.path.isdir(root_dirname)   # ...Murphy and her ironclad Law

        # Append a path separator to this directory if needed.
        root_dirname = root_dirname.rstrip(os.path.sep) + os.path.sep

        # Test whether each path component split from this pathname is valid or
        # not, ignoring non-existent and non-readable path components.
        for pathname_part in pathname.split(os.path.sep):
            try:
                os.lstat(root_dirname + pathname_part)
            # If an OS-specific exception is raised, its error code
            # indicates whether this pathname is valid or not. Unless this
            # is the case, this exception implies an ignorable kernel or
            # filesystem complaint (e.g., path not found or inaccessible).
            #
            # Only the following exceptions indicate invalid pathnames:
            #
            # * Instances of the Windows-specific "WindowsError" class
            #   defining the "winerror" attribute whose value is
            #   "ERROR_INVALID_NAME". Under Windows, "winerror" is more
            #   fine-grained and hence useful than the generic "errno"
            #   attribute. When a too-long pathname is passed, for example,
            #   "errno" is "ENOENT" (i.e., no such file or directory) rather
            #   than "ENAMETOOLONG" (i.e., file name too long).
            # * Instances of the cross-platform "OSError" class defining the
            #   generic "errno" attribute whose value is either:
            #   * Under most POSIX-compatible OSes, "ENAMETOOLONG".
            #   * Under some edge-case OSes (e.g., SunOS, *BSD), "ERANGE".
            except OSError as exc:
                if hasattr(exc, 'winerror'):
                    if exc.winerror == ERROR_INVALID_NAME:
                        return False
                elif exc.errno in {errno.ENAMETOOLONG, errno.ERANGE}:
                    return False
    # If a "TypeError" exception was raised, it almost certainly has the
    # error message "embedded NUL character" indicating an invalid pathname.
    except TypeError as exc:
        return False
    # If no exception was raised, all path components and hence this
    # pathname itself are valid. (Praise be to the curmudgeonly python.)
    else:
        return True
    # If any other exception was raised, this is an unrelated fatal issue
    # (e.g., a bug). Permit this exception to unwind the call stack.
    #
    # Did we mention this should be shipped with Python already?



################################################################################################
################################################################################################
################################################################################################




def get_device_from_pth(pth, raise_on_none=False):
    """helper function to get device from a path
        e.G.:
            '/global_aux/20220613_0616_IDP_1_MWS.zip' 
                -> 'MWS'
    """
    fname = os.path.basename(pth)
    
    r = re.findall(r"_[A-Z0-9]{3,}\.", fname)    
    ret = str(r[-1][1:-1]) if r else None

    if ret is None:
        _log.warning('Could not determine device from:')
        _log.warning(fname)

        if raise_on_none:
            raise Exception(f'Could not determine device from: "{fname}"')

    return ret



def get_id_data_from_path(s):
    """helper function to get id from a path

        e.G.:
            '.../20220613_0616_IDD_1_...' 
                -> 1 (int)
    """
    r = re.findall(r"IDD_[0-9]+", s)    
    return int(r[-1][4:]) if r else None
    

def mkdir(pth, raise_ex=False, verbose=False):
    try:
        if not os.path.exists(pth):
            if verbose:
                _log.info('Creating dir because it does not exist: ' + pth)
            os.makedirs(pth, exist_ok=True)
            path = Path(pth)
            path.mkdir(parents=True, exist_ok=True)
            return str(path).replace('\\', '/').replace('//', '/')

    except Exception as err:
        _log.error(err)
        if raise_ex:
            raise
    return None

def join(*parts):
    return os.path.join(*parts).replace('\\', '/').replace('//', '/')

timeformat_str = '%Y%m%d_%H%M'

def mk_out_dir(dir, is_exp):
    _mkdir = lambda p: mkdir(p, raise_ex=True)

    pths = []
    pths.append(mkdir(dir))
    if is_exp:
        pths.append(_mkdir(join(dir, 'data')))
        pths.append(_mkdir(join(dir, 'data_raw')))
        pths.append(_mkdir(join(dir, 'data_aux')))
    return pths

def get_ana_save_filepath(basedir:str, dtime:datetime.datetime, analysis_id:int, antenna_id:str, analysis_name:str, tag:str=None, make_dir=False):

    fulldir = join(basedir, antenna_id)
    time = dtime.strftime(timeformat_str)
    tag = ('_' + tag.strip().replace(' ', '')) if tag else ''
    fname = f'{time}_{analysis_id}_{antenna_id}_ana_{analysis_name}{tag}.ipynb'
    fullpath = join(fulldir, fname)
    if make_dir:
        mkdir(fulldir, raise_ex=True)

    return fullpath

def get_exp_save_filepath(basedir:str, dtime:datetime.datetime, experiment_id:int, antenna_id:str, experiment_name:str, make_dir=False):
    fulldir = get_exp_save_dir(basedir, dtime, experiment_id, antenna_id, experiment_name, make_dir=make_dir)
    fname = fulldir.split('/')[-1] + '.ipynb'
    fullpath = join(fulldir, fname)
    return fullpath


def get_exp_save_dir(basedir:str, dtime:datetime.datetime, experiment_id:int, antenna_id:str, experiment_name:str, tag:str=None, make_dir=False):
    time = dtime.strftime(timeformat_str)
    tag = ('_' + tag.strip().replace(' ', '')) if tag else ''
    subdir = ''
    if antenna_id:
        subdir += f'{antenna_id}/'
    
    subdir += f"{time}_{experiment_id}_{antenna_id}_exp_{experiment_name}{tag}"
    fulldir = join(basedir, subdir)
    if make_dir:
        mkdir(fulldir, raise_ex=True)
    return fulldir


def get_exp_save_pth_datafile(basedir:str, dtime:datetime.datetime, experiment_id:int, meas_id:str, device_key:str, tag:str=None, extension:str='.csv', make_dir=False):
    time = dtime.strftime(timeformat_str)
    subdir = f"data_raw"
    tag = ('_' + tag.strip().replace(' ', '')) if tag else ''
    fname = f'{time}_IDP_{experiment_id}_IDD_{meas_id}{tag}_{device_key}{extension}'
    fulldir = join(basedir, subdir)
    fullpath = join(fulldir, fname)
    if make_dir:
        mkdir(fulldir, raise_ex=True)
    return fullpath


def get_exp_aux_save_dir(dtime:datetime.datetime, experiment_id:int, antenna_id:str, experiment_name:str, make_dir=False):
    basedir = get_exp_save_dir(dtime, experiment_id, antenna_id, experiment_name, make_dir=False)
    
    subdir = f"data_aux"
    fulldir = join(basedir, subdir)
    if make_dir:
        mkdir(fulldir, raise_ex=True)
    return fulldir


def get_exp_aux_save_filepath(basedir, dtime:datetime.datetime, experiment_id:int, device_key:str, extension:str='.csv', make_dir=False):
    time = dtime.strftime(timeformat_str) if isinstance(dtime, datetime.datetime) else dtime
    fname = f'{time}_IDP_{experiment_id}_{device_key}{extension}'
    fulldir = join(basedir, fname)
    return fulldir
