import pandas as pd


def generate_tv_table(tv_file_path):
    xl = pd.ExcelFile(tv_file_path)

    tv_table_dict = dict()
    for sheet_name in xl.sheet_names[2:]:
        tv_table_dict[sheet_name] = xl.parse(sheet_name, skiprows=4)
    for k, v in tv_table_dict.items():
        v.dropna(how='all').to_csv(f'tv{str(k).zfill(3)}.csv')


def generate_tr_table(tr_file_path):
    xl = pd.ExcelFile(tr_file_path)

    tr_table_dict = dict()
    for sheet_name in xl.sheet_names[2:]:
        tr_table_dict[sheet_name] = xl.parse(sheet_name, skiprows=4)

    for k, v in tr_table_dict.items():
        if '-' in k:
            k1, k2 = k.split('-')
            v.dropna(how='all').to_csv(f'tr{str(k1).zfill(3)}-{k2}.csv',index=False)
        else:
            v.dropna(how='all').to_csv(f'tr{str(k).zfill(3)}.csv',index=False)

