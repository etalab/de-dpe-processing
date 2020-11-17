import pandas as pd
def postprocessing_td001(td001):
    # NORMALISATION METHODE CALCUL.
    td001['nom_methode_dpe_norm'] = 'NON DEFINI'

    nom_dpe = td001.nom_methode_dpe.copy().str.lower()

    is_3cl = nom_dpe.str.contains('cl')
    is_facture = nom_dpe.str.contains('facture')
    is_thc = nom_dpe.str.startswith('th')
    is_vierge = nom_dpe.str.contains('vierge')
    s_version = td001.version_methode_dpe.str.lower().fillna('')
    v2012 = s_version.str.contains('2012|1.3')

    td001.loc[is_3cl & v2012, 'nom_methode_dpe_norm'] = '3CL 2012'
    td001.loc[is_3cl & (~v2012), 'nom_methode_dpe_norm'] = '3CL 2005'
    td001.loc[is_facture, 'nom_methode_dpe_norm'] = 'FACTURE'
    td001.loc[is_thc, 'nom_methode_dpe_norm'] = 'THBCE(RT2012)/THC(RT2005)'
    td001.loc[is_vierge, 'nom_methode_dpe_norm'] = 'DPE vierge'
    # periode_construction = pd.cut(td001.annee_construction.astype(float),
    #                               [-100000, 1800, 1948, 1970, 1988, 1999, 2005, 2012, 2020, 2100],
    #                               labels=['bad inf', '<1948', '1949-1970', '1970-1988', '1989-1999', '2000-2005',
    #                                       '2006-2012', '>2012', 'bad sup'])
    return td001