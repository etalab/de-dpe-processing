import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql
import json
from pathlib import Path
import yaml
import os

curdir = Path(os.curdir).absolute()
os.chdir(curdir.parent.parent)
try:
    from utils import create_db_engine
except:
    def create_db_engine(yml_path):
        """
        Load database parameter from a YML file
        and return an sqlalchemy PostgreSQL engine object
        """

        # Load config file content
        with open(yml_path, "r") as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.SafeLoader)

            # constantes
            HOST = cfg['db']['host']
            PORT = cfg['db']['port']
            DATABASE = cfg['db']['db_name']
            USER = cfg['db']['user']
            PASSWORD = cfg['db']['password']

        # Init connection to db on POSTGRES db
        return sqlalchemy.create_engine('postgresql://{}:{}@{}:{}/{}'.format(
            USER, PASSWORD, HOST, PORT, DATABASE), encoding='utf8')

os.chdir(curdir)
yml_path = Path.home() / 'work' / 'config.yml'

# PostgreSQL engine creation
engine = create_db_engine(yml_path)
sql_config = dict()
sql_config['schemas']={'dpe_raw_schema_name':'adedpe202006',
               'dpe_out_schema_name':"z_tmp_ab"}

td001_cols = 'td001_dpe.id as td001_dpe_id,td001_dpe.annee_construction as annee_construction,td001_dpe.tv016_departement_id as tv016_departement_id'
