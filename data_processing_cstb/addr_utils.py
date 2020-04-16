import pandas as pd
import re
def build_concat_addr_from_table(df, addr_cols):
    def format_addr_col_for_concat(x):
        if isinstance(x, float):
            return ''
        elif x.strip() != '' and x.strip()!='nan':
            return x + ' '
        elif x.strip()=='nan':
            return ''
        else:
            return x

    df = df.copy()
    df[addr_cols] = df[addr_cols].fillna('')
    s_addr = pd.Series(index=df.index).fillna('')
    df['adresse_concat'] = ''
    for col in addr_cols:
        s_addr += df[col].apply(lambda x: format_addr_col_for_concat(x))
    return s_addr




def clean_addr_serie(s):
    s = s.fillna('')
    # remove useless spaces
    s = s.apply(lambda x: clean_addr_string(x))
    return s


def clean_addr_string(x):
    bad_chars_to_space = ['"', "'",  # these char in addr field generate errors on addok
                          ",",  # removed by precaution since we dump comma sep csv and we dont want quotechar
                          "\\n", "\\t",  #
                          "\n", "\t",  # remove carriage return/tab
                          "/", "\\", "(", ")"  # remove special separators characters and ()
                                          "[", "]", "_",  # remove special separators characters and ()

                          ]
    bad_addr_words = [
        "APPARTEMENT ",
        "Appartement ",
        "Apt ",
        "Apt.",
        "apt ",
        "apt.",
        "non communiquée",
        "LOTISSEMENT ",
        "Lotissement ",
        "lotissement ",
        "LOT ",
        "Lot ",
        "lot ",
        "NC ",
        "_", ]
    x = x.rstrip()

    for bad in bad_chars_to_space:
        x = x.replace(bad, " ")
    for bad_word in bad_addr_words:
        x = x.replace(bad_word, " ")

    x = x.strip()
    x = re.sub(' +', ' ', x)
    return x



def build_communes_possibilities_using_ban(df_addr,df_match_ban_name_com,df_cp_com_flat):

    addr_table_cols = df_addr.columns
    required_cols = set(['code_insee','code_postal','commune'])-set(df_addr)
    if len(required_cols) >0:
        raise KeyError(f"missing {required_cols} in source dataframe")    
    

    df_addr_raw = df_addr.copy()
    df_addr_raw['com_source']='raw'


    df_addr_com = df_addr.copy()


    # In[187]:


    df_addr_com=df_addr_com.merge(df_match_ban_name_com[['commune','code_insee','code_postal','nom_com_ban_name']],
                              on=['commune'],how='left',suffixes=('','_ban_name'))


    # In[188]:


    df_addr_com['commune']=df_addr_com.nom_com_ban_name
    df_addr_com['code_insee']=df_addr_com.code_insee_ban_name
    df_addr_com['code_postal']=df_addr_com.code_postal_ban_name


    # In[189]:


    df_addr_com=df_addr_com[addr_table_cols].dropna(subset=['commune'])
    df_addr_com['com_source']='match_ban_name'


    # ## association ban sur combinaison cp/code insee
    #
    # association de la combinaison cp/code insee

    # In[190]:


    df_addr_code_cp = df_addr.copy()

    df_addr_code_cp=df_addr_code_cp.merge(df_cp_com_flat[['code_postal','code_insee','nom_commune']],
                              on=['code_postal','code_insee'],how='left',suffixes=('','_ban_code_cp'))

    df_addr_code_cp['commune']=df_addr_code_cp.nom_commune


    print(f'assoc ban cp code insee commune  : {1-df_addr_code_cp.commune.isnull().mean()}')
    print(f"cela veut dire qu'on a une combinaison viable de cp/code commune dans {(1-df_addr_code_cp.commune.isnull().mean())*100} % des cas")

    df_addr_code_cp=df_addr_code_cp[addr_table_cols].dropna(subset=['commune'])

    df_addr_code_cp['com_source']='match_ban_code_cp'


    # ## association ban sur cp uniquement

    # In[191]:


    from fuzzywuzzy import fuzz

    df_addr_cp = df_addr.copy()

    df_addr_cp=df_addr_cp.merge(df_cp_com_flat[['code_postal','code_insee','nom_commune']],
                              on=['code_postal'],how='left',suffixes=('','_ban_cp'))


    df_addr_cp['code_insee']=df_addr_cp.code_insee_ban_cp
    df_addr_cp = df_addr_cp.dropna(subset=['commune','nom_commune'])
    df_addr_cp['score_nom_com_cp'] = df_addr_cp.apply(lambda row: fuzz.partial_ratio(row.commune.lower().strip().replace(' ','-'),
                                                                                                     row.nom_commune.lower().strip().replace(' ','-')) ,axis=1)





    df_addr_cp=df_addr_cp.sort_values('score_nom_com_cp',
                                                ascending=False).drop_duplicates('id')

    df_addr_cp['commune']=df_addr_cp['nom_commune']

    df_addr_cp=df_addr_cp[addr_table_cols].dropna(subset=['commune'])

    df_addr_cp['com_source']='match_ban_cp'


    #
    #
    # ## association par code insee uniquement

    # In[192]:


    df_addr_code = df_addr.copy()

    df_addr_code=df_addr_code.merge(df_cp_com_flat.drop_duplicates('code_insee')[['code_postal','code_insee','nom_commune']],
                              on=['code_insee'],how='left',suffixes=('','_ban_code'))


    df_addr_code['commune']=df_addr_code.nom_commune

    print(f'assoc ban code commune  : {1-df_addr_code.commune.isnull().mean()}')

    print(f"cela veut dire qu'on a un code commune insee qui match avec une commune dans {(1-df_addr_code.commune.isnull().mean())*100} % des cas")


    df_addr_code=df_addr_code[addr_table_cols].dropna(subset=['commune'])

    df_addr_code['com_source']='match_ban_code'

    df_addr = pd.concat([df_addr_raw,df_addr_com,df_addr_code_cp,df_addr_cp,df_addr_code])

    return df_addr