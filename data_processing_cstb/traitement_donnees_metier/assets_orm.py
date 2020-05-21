from pathlib import Path
import pandas as pd

def load_tv_table_dict(tv_dir_path):

    tv_dir_path = Path(tv_dir_path)
    tv_table_dict =dict()
    for table_path in tv_dir_path.iterdir():
        tv_table_dict[table_path.name.split('.csv')[0] ] =pd.read_csv(table_path,index_col=0).rename(columns={"Code":'code'}).astype('category')

    return tv_table_dict

def load_tr_table_dict(tr_dir_path):

    tr_dir_path = Path(tr_dir_path)
    tr_table_dict =dict()
    for table_path in tr_dir_path.iterdir():
        tr_table_dict[table_path.name.split('.csv')[0]] =pd.read_csv(table_path,index_col=0).rename(columns={"Code":'code'}).astype('category')

    return tr_table_dict


class DPEMetaData():

    def __init__(self ,assets_dir=Path(__file__).parent/'assets'):

        self.tv_table_dict = load_tv_table_dict(assets_dir/'tv_tables')
        self.tr_table_dict = load_tr_table_dict(assets_dir/'tr_tables')


    def merge_all_tr_table(self, table):
        table = table.copy()
        tr_cols = [col for col in table.columns if col.startswith('tr')]
        for tr_col in tr_cols:
            table = self.merge_tr_table(table, tr_col)
        return table

    def merge_tr_table(self, table, tr_col):
        tr_name = tr_col.split('_')[0].lower()
        tr_table = self.tr_table_dict[tr_name]
        tr_table = tr_table.rename(columns={'code': tr_col})
        table[tr_col] = ((tr_name + '_' + table[tr_col].fillna(pd.NA).astype(str).str.zfill(3)).str.upper()).astype('category')
        tr_code = tr_col.split('_')[0]
        cols = tr_table.columns.tolist()
        cols[0] = tr_col
        cols[1:] = [tr_code + '_' + col for col in cols[1:]]
        tr_table.columns = cols
        table = table.merge(tr_table, on=tr_col, how='left')
        table[cols]=table[cols].astype('category')
        table.index = table.index.astype(str)

        return table

    def merge_all_tv_table(self, table):
        table = table.copy()
        tv_cols = [col for col in table.columns if col.startswith('tv')]
        for tv_col in tv_cols:
            table = self.merge_tv_table(table, tv_col)
        return table

    def merge_tv_table(self, table, tv_col):
        tv_name = tv_col.split('_')[0].lower()
        tv_table = self.tv_table_dict[tv_name]
        tv_table = tv_table.rename(columns={'code': tv_col})
        tv_code = tv_col.split('_')[0]
        table[tv_col] = ((tv_name + '_' + table[tv_col].astype('string').str.zfill(3)).str.upper()).astype('category')
        cols = tv_table.columns.tolist()
        cols[0] = tv_col
        cols[1:] = [tv_code + '_' + col for col in cols[1:]]
        tv_table.columns = cols
        table = table.merge(tv_table, on=tv_col, how='left')
        table[cols]=table[cols].astype('category')
        table.index = table.index.astype(str)
        return table

