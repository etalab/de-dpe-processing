from pathlib import Path
import pandas as pd
from utils import merge_without_duplicate_columns


def load_trtv_table_dict(trtv_dir_path):
    trtv_dir_path = Path(trtv_dir_path)
    trtv_table_dict = dict()
    for table_path in trtv_dir_path.iterdir():
        table_name = table_path.name.split('.csv')[0]
        table = pd.read_csv(table_path,dtype=str).astype('category')
        table_ref = table_path.name.split('_')[0]
        table.columns = [table_ref + '_' + el if not el.startswith(('tr0', 'tv0')) else el for el in table.columns]
        table = table.rename(columns={f'{table_ref}_id': f'{table_name}_id'})
        trtv_table_dict[table_name] = table
    for table_name, table in trtv_table_dict.items():
        table_id = table_name + '_id'
        foreign_ids = [col for col in table if col.endswith('_id') and not col == table_id]
        for foreign_id in foreign_ids:
            foreign_table_name = foreign_id.split('_id')[0]
            foreign_table = trtv_table_dict.get(foreign_table_name, None)
            if foreign_table is None:
                print(foreign_table_name + ' not found')
            else:
                table = table.merge(foreign_table, on=foreign_id, how='left')
        cols = [el for el in table if not any(s in el for s in ['simulateur', 'simu_ordre', 'id', 'est_efface'])]
        cols.append(f'{table_name}_id')
        trtv_table_dict[table_name] = table[cols]
    return trtv_table_dict


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class DPETrTvTables(metaclass=Singleton):

    def __init__(self, assets_dir=Path(__file__).parent / 'assets'):

        self.trtv_table_dict = load_trtv_table_dict(assets_dir / 'tv_tables')
        self.trtv_table_dict.update(load_trtv_table_dict(assets_dir / 'tr_tables'))

    def merge_all_tr_tables(self, table):
        table = table.copy()
        tr_cols = [col for col in table.columns if col.startswith('tr')]
        for tr_col in tr_cols:
            table = self.merge_trtv_table(table, tr_col)
        return table

    def merge_all_tv_tables(self, table):
        table = table.copy()
        tv_cols = [col for col in table.columns if col.startswith('tv')]
        for tv_col in tv_cols:
            table = self.merge_trtv_table(table, tv_col)
        return table

    def merge_trtv_table(self, table, trtv_col):

        trtv_table_name = trtv_col.split('_id')[0]
        trtv_table = self.trtv_table_dict[trtv_table_name]
        table = merge_without_duplicate_columns(table, trtv_table,
                                                on=trtv_col, merge_kwargs=dict(how='left'))
        table.index = table.index.astype(str)
        cols = trtv_table.columns.tolist()
        table[cols] = table[cols].astype('category')

        return table
