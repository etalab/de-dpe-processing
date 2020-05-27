import pandas as pd
import numpy as np


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


def agg_pond_avg(table, value_col,pond,by,bool_filter_col=None, bool_filter_not=False):
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
    pond_col = 'pond9999999'
    table = _prep_agg_pond(table, pond, bool_filter_col, pond_col, bool_filter_not)
    grp = table.groupby(by).apply(lambda x: _agg_pond_avg_core(x, value_col, pond_col))
    if pond_col in grp:
        del grp[pond_col]
    return grp


def _agg_pond_avg_core(x, value_col, pond_col):
    pond_col_sum = x[pond_col].sum()

    if pond_col_sum == 0:
        return x[value_col].mean()
    else:
        return (x[value_col] * x[pond_col]).sum() / (x[pond_col].sum())


def agg_pond_top_freq(table,enum_col,pond, by, bool_filter_col=None, bool_filter_not=False):
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
    tuple_list = list()
    for index, row in table.groupby([by, enum_col])[pond_col].sum().unstack().iterrows():
        row = row.sort_values(ascending=False).dropna()
        if row.shape[0] > 0:
            tuple_list.append((index, row.index[0]))
        else:
            tuple_list.append((index, np.nan))
    s = pd.Series([el[1] for el in tuple_list], [el[0] for el in tuple_list])
    s.index.name = by

    return s