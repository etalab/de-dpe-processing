import zipfile
from io import BytesIO, StringIO
from pathlib import Path
from generate_dpe_annexes_scripts.gorenove_scripts import gorenove_types
import json
import numpy as np
import pandas as pd
if __name__ == '__main__':
    cat_variables = [k for k, v in gorenove_types.items() if v == 'category']

    json_meta_gorenove = dict()

    json_meta_gorenove['version'] = '2.0.0'
    json_meta_gorenove['types'] = gorenove_types
    json_meta_gorenove['enums'] = {k: [] for k in cat_variables}

    root_dir = Path(r'D:\data\dpe_full')
    depts_dir = root_dir / Path('annexes_cstb')
    td001_zipped_dir = root_dir / Path('annexes_cstb_zipped_for_gorenove')
    td001_zipped_dir.mkdir(exist_ok=True)
    for dept in depts_dir.iterdir():
        if dept.is_dir():
            zipped_file = td001_zipped_dir / f'{dept.name}.zip'
            mf = BytesIO()
            with zipfile.ZipFile(mf, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
                for a_file in dept.iterdir():
                    if a_file.name.startswith('td001_gorenove'):
                        table = pd.read_csv(a_file, index_col=0, dtype=str)
                        table = table.replace('nan', np.nan)
                        table = table.rename(columns={'u_mur_ext_y': 'u_mur_ext'})
                        for col in cat_variables:
                            json_meta_gorenove['enums'][col] += table[col].dropna().unique().tolist()
                            json_meta_gorenove['enums'][col] = list(set(json_meta_gorenove['enums'][col]))
                        zf.writestr('td001_agg_synthese_gorenove.csv', table.to_csv())

            with open(zipped_file, "wb") as f:  # use `wb` mode
                f.write(mf.getvalue())
    with open(td001_zipped_dir/'meta_data.json','w') as f:
        json.dump(json_meta_gorenove,f,indent=4)

