import os
import logging

config = dict()
paths = {'DPE_DEPT_ANNEXE_PATH': os.environ.get('DPE_DEPT_ANNEXE_PATH', r'D:\data\dpe_full\annexes_cstb'),
         'DPE_DEPT_PATH': os.environ.get('DPE_DEPT_PATH', r'D:\data\dpe_full\depts'),
         'ES_SERVER_PATH': os.environ.get('ES_SERVER_PATH', os.environ[
                      'USERPROFILE'] + r'\apps\elasticsearch-7.10.2-windows-x86_64\elasticsearch-7.10.2\bin\elasticsearch.bat'),

         }

# logger
FORMAT = "%(asctime)s %(name)s %(levelname)-8s %(message)s"
logger = logging.getLogger('generate_dpe_annexes')
log_level = os.environ.get("GENEREATE_DPE_ANNEXES_LOG_LEVEL", "DEBUG")
logger.setLevel(log_level)
formatter = logging.Formatter(FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
ch = logging.StreamHandler()
ch.setLevel(log_level)
ch.setFormatter(formatter)
if (logger.hasHandlers()):
    logger.handlers.clear()
logger.addHandler(ch)
logger.propagate = 0

config['logger'] = logger
# multiprocessing

config['multiprocessing']={'is_multiprocessing':False ,
                           'nb_proc':3}