import pandas as pd
import numpy as np
def postprocessing_td001(td001,td002,td007,td012,td016,td017):

    # booleen modele sous jacent
    td016['is_facture_table'] = True
    td017['is_neuf_table'] = True
    td007['is_paroi_opaque_table'] = True
    td012['is_systeme_ch_table'] = True
    td002['is_conso_table'] = True

    # MERGE tables modele sous jacent
    td001 = td001.merge(td002.groupby('td001_dpe_id').is_conso_table.max().reset_index(), how='left')
    td001 = td001.merge(td007.groupby('td001_dpe_id').is_paroi_opaque_table.max().reset_index(), how='left')
    td001 = td001.merge(td012.groupby('td001_dpe_id').is_systeme_ch_table.max().reset_index(), how='left')
    td001 = td001.merge(td016.groupby('td001_dpe_id').is_facture_table.max().reset_index(), how='left')
    td001 = td001.merge(td017.groupby('td001_dpe_id').is_neuf_table.max().reset_index(), how='left')

    td001[['is_conso_table', 'is_facture_table', 'is_neuf_table', 'is_paroi_opaque_table', 'is_systeme_ch_table']] = td001[
        ['is_conso_table', 'is_facture_table', 'is_neuf_table', 'is_paroi_opaque_table', 'is_systeme_ch_table']].fillna(False)

    td001['is_3cl'] = (td001.is_paroi_opaque_table) & (td001.is_systeme_ch_table)

    td001['coherence_data_model'] = True
    bool_ = td001.is_facture_table == True
    td001.loc[bool_, 'coherence_data_model'] = (td001.loc[bool_, 'is_neuf_table'] == False) & (td001.loc[bool_, 'is_3cl'] == False)
    bool_ = td001.is_neuf_table == True
    td001.loc[bool_, 'coherence_data_model'] = (td001.loc[bool_, 'is_facture_table'] == False) & (td001.loc[bool_, 'is_3cl'] == False)
    bool_ = td001.is_3cl == True
    td001.loc[bool_, 'coherence_data_model'] = (td001.loc[bool_, 'is_facture_table'] == False) & (td001.loc[bool_, 'is_neuf_table'] == False)

    td001['methode_dpe_from_data_model'] = 'NON DEFINI'

    bool_propre = td001.coherence_data_model == True
    bool_ = bool_propre & (td001.is_3cl == True)
    td001.loc[bool_, 'methode_dpe_from_data_model'] = '3CL'
    bool_ = bool_propre & (td001.is_neuf_table == True)
    td001.loc[bool_, 'methode_dpe_from_data_model'] = 'THBCE(RT2012)/THC(RT2005)'
    bool_ = bool_propre & (td001.is_facture_table == True)
    td001.loc[bool_, 'methode_dpe_from_data_model'] = 'FACTURE'

    td001["type_batiment"] = td001.tr002_type_batiment_id.replace({
        '1': 'maison individuelle',
        '2': 'appartement',
        '3': "immeuble collectif d'habitation",
        "4": "tertiaire",
        "5": "tertiaire"

    })

    num_versions_rt2012 = ['6300',
                           '7502',
                           '7100',
                           '7000',
                           '7503',
                           '7400',
                           '7200',
                           '7501',
                           '8100',
                           '1163',
                           'RT2012',
                           '7.5.0.2',
                           '8000',
                           'RT2005',
                           '6.3.0.0',
                           '7.3.0.0',
                           '7.2.234.6579',
                           '7.0.0.0',
                           '7.5.0.3',
                           '7.1.0.0',
                           '6100',
                           '7.2.0.0',
                           '7.5.0.1',
                           '8.1.0.0',
                           '8.0.0.0',
                           '7300',
                           '7500',
                           '1161',
                           '7.4.0.0',
                           '7.1.112.6166',
                           '1.1.6.3',
                           '1.1.3',
                           '1.3',
                           '7.5.0.0',
                           '1.1.6.1',
                           '5100',
                           '1152']

    td001['nom_methode_dpe_norm'] = 'NON DEFINI'

    nom_dpe = td001.nom_methode_dpe.copy().str.lower()
    nom_methode_etude_thermique = td001.nom_methode_etude_thermique.str.lower()


    td001['classe_consommation_energie_norm']=td001.classe_consommation_energie.copy()
    td001['classe_estimation_ges_norm'] = td001.classe_consommation_energie.copy()
    not_valid = ~td001.classe_consommation_energie.isin(['A', 'B', 'C', 'D', 'E', 'F', 'G'])
    td001.loc[not_valid, 'classe_consommation_energie_norm'] = 'N'
    not_valid = ~td001.classe_estimation_ges.isin(['A', 'B', 'C', 'D', 'E', 'F', 'G'])
    td001.loc[not_valid, 'classe_estimation_ges_norm'] = 'N'

    is_3cl = nom_dpe.str.contains('cl')
    is_facture = nom_dpe.str.contains('facture')
    is_thc = (nom_dpe.str.startswith('th')) | (nom_methode_etude_thermique.str.startswith('th')) | (td001.version_methode_etude_thermique.isin(num_versions_rt2012))
    is_vierge = nom_dpe.str.contains('vierge')
    is_classe_vierge = td001.classe_consommation_energie_norm=="N"

    s_version = td001.version_methode_dpe.str.lower().fillna('')
    v2012 = s_version.str.contains('2012|1.3')

    td001.loc[is_3cl & v2012, 'nom_methode_dpe_norm'] = '3CL 2012'
    td001.loc[is_3cl & (~v2012), 'nom_methode_dpe_norm'] = '3CL 2005'
    td001.loc[is_facture, 'nom_methode_dpe_norm'] = 'FACTURE'
    td001.loc[is_thc, 'nom_methode_dpe_norm'] = 'THBCE(RT2012)/THC(RT2005)'
    td001.loc[is_vierge | is_classe_vierge, 'nom_methode_dpe_norm'] = 'DPE vierge'
    periode_construction = pd.cut(td001.annee_construction.astype(float),
                                  [-np.inf, 1700, 1948, 1970, 1988, 1999, 2005, 2012, 2020, np.inf],
                                  labels=['bad inf', '<1948', '1949-1970', '1970-1988', '1989-1999', '2000-2005',
                                          '2006-2012', '>2012', 'bad sup'])

    td001['periode_construction'] = periode_construction

    valid_combination = [
        ('3CL 2005', '3CL'),
        ('3CL 2012', '3CL'),
        ('DPE vierge', 'NON DEFINI'),
        ('FACTURE', 'FACTURE'),
        ('THBCE(RT2012)/THC(RT2005)', 'THBCE(RT2012)/THC(RT2005)')
    ]

    td001['coherence_data_methode_dpe'] = False

    for norm, data in valid_combination:
        bool_ = (td001.nom_methode_dpe_norm == norm) & (td001.methode_dpe_from_data_model == data)
        td001.loc[bool_, 'coherence_data_methode_dpe'] = True

    return td001