import pandas as pd
import numpy as np
import uuid
import unicodedata
import re

def clean_str(x):
    x = x.strip()
    x = re.sub(' +', ' ', x)
    return x


def strip_accents(s):
    """
    remove accents from a string.
    Parameters
    ----------
    s :str
    string

    Returns
    -------

    """
    s = s.replace('\xb0C', 'degC')
    s = s.replace('\xb5', 'u')
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')


def intervals_to_category(s, cat_dict):
    """
    transform numerical serie to categorical str enum comparing numerical values with intervals defining the categories.
    Parameters
    ----------
    s : pd.Series
    cat_dict :dict
    a dictionary that have as keys enum categories and as values a tuple of an interval of numerical values.

    Returns
    -------

    """
    s_cat = s.copy()
    for k, v in cat_dict.items():
        s_cat[s.between(*v)] = k
    return s_cat


def _prep_agg_pond(table, pond, bool_filter_col, pond_col, bool_filter_not):
    """

    Parameters
    ----------
    table : pd.DataFrame
    by : str or list
    pandas.DataFrame.Groupby argument
    pond : str,list
    column or columns containing numeric values
    bool_filter_col : str
    column containing a boolean array to filter data
    bool_filter_not : bool
    if true take the negative of the boolean array instead
    pond_col:str
    name of the pond column.

    Returns
    -------
    table : pd.DataFrame
    transformed table

    """
    if bool_filter_col is not None:
        if bool_filter_not:
            table = table.loc[~table[bool_filter_col].astype(bool), :]
        else:
            table = table.loc[table[bool_filter_col].astype(bool), :]
    if isinstance(pond, str):
        table[pond_col] = table[pond]
    elif isinstance(pond, (list, tuple)):
        table[pond_col] = 1
        for col in pond:
            table[pond_col] = table[pond_col] * table[col]
    else:
        raise BaseException('pond must be str,list or tuple not {}'.format(type(pond)))
    return table


def agg_pond_avg(table, value_col, pond, by, bool_filter_col=None, bool_filter_not=False):
    """
    function to make an average ponderate serie from a table column
    Parameters
    ----------
    table : pd.DataFrame

    value_col : str
    column containing numeric values
    pond : str,list
    column or columns containing numeric values
    by : str or list
    pandas.DataFrame.Groupby argument
    bool_filter_col : str
    column containing a boolean array to filter data
    bool_filter_not : bool
    if true take the negative of the boolean array instead

    Returns
    -------
    grp : pd.Series
    serie of ponderate avg of value_col

    """
    pond_col = str(uuid.uuid4())
    table = _prep_agg_pond(table, pond, bool_filter_col, pond_col, bool_filter_not)
    pond_value_col_temp = str(uuid.uuid4())
    table[pond_value_col_temp] = table[pond_col] * table[value_col]
    null = table[pond_value_col_temp].isnull()
    null = null | table[pond_col].isnull()
    table.loc[null, [pond_col, pond_value_col_temp]] = np.nan
    grp = table.groupby(by)[[pond_col, pond_value_col_temp]].sum()
    grp[grp[pond_col] <= 0] = np.nan
    s_grp = grp[pond_value_col_temp] / grp[pond_col]
    del table[pond_col]
    del table[pond_value_col_temp]

    return s_grp


def agg_pond_top_freq(table, enum_col, pond, by, bool_filter_col=None, bool_filter_not=False):
    """
    function to make an topfreq ponderate serie from a table column

    Parameters
    ----------
    table : pd.DataFrame

    enum_col : str
    column containing enumerator values
    pond : str,list
    column or columns containing numeric values
    by : str or list
    pandas.DataFrame.Groupby argument
    bool_filter_col : str
    column containing a boolean array to filter data
    bool_filter_not : bool
    if true take the negative of the boolean array instead

    Returns
    -------
    grp : pd.Series
    serie of ponderated topfreq of enum_col

    """

    pond_col = 'pond9999999'
    table = _prep_agg_pond(table, pond, bool_filter_col, pond_col, bool_filter_not)
    grp = table.groupby([by, enum_col])[pond_col].sum()
    s = grp.reset_index().sort_values([by, pond_col], ascending=False).drop_duplicates(subset=by).set_index(by)[
        enum_col]

    return s


def affect_lib_by_matching_score(txt, lib_dict):
    """
    function that rank matching score for associating a string
    to an enum depending on the matching with different keywords associated to that enum. The matching score is the sum
    of the occurences of all keywords. if a keyword is missing it decrease the score by 1.

    Parameters
    ----------
    txt :str
    text to be matched
    lib_dict :dict
    key : enum , value is a list of keyword or tuple of keywords.
    if there is tuple of keyword it match any of the keywords inside the tuple

    Returns
    -------

    """

    def compare_(txt, comp):
        if isinstance(comp, tuple):
            count = np.max([txt.count(x) for x in comp])
        else:
            count = txt.count(comp)

        if count > 0:
            return count
        else:
            return -1

    comp_score_dict = dict()
    for k, v in lib_dict.items():
        comp_score_dict[k] = np.sum([compare_(txt, el) for el in v])

    comp = pd.Series(comp_score_dict).sort_values(ascending=False)
    if comp.max() > 0:
        comp = comp.loc[comp == comp.max()]

        affectation = comp.sort_index().index[0]  # sorting index in case of conflicts
        return affectation
    else:
        return 'non affecté'
