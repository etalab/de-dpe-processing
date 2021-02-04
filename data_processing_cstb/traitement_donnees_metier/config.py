import os

paths = {'DPE_DEPT_ANNEXE_PATH': os.environ.get('DPE_DEPT_ANNEXE_PATH', r'D:\data\dpe_full\annexes_cstb'),
         'DPE_DEPT_PATH': os.environ.get('DPE_DEPT_PATH', r'D:\data\dpe_full\depts'),
         'ES_SERVER_PATH': os.environ.get('ES_SERVER_PATH', os.environ[
                      'USERPROFILE'] + r'\apps\elasticsearch-7.10.2-windows-x86_64\elasticsearch-7.10.2\bin\elasticsearch.bat'),

         }
nb_proc = 3

