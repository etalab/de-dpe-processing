import pandas as pd
import numpy as np
from utils import agg_pond_avg, agg_pond_top_freq

td007_types = {'id': 'str',
               'td006_batiment_id': 'str',
               'tr014_type_parois_opaque_id': 'str',
               'reference': 'str',
               'deperdition_thermique': 'float',
               'tv001_coefficient_reduction_deperditions_id': 'category',
               'tv002_local_non_chauffe_id': 'category',
               'coefficient_transmission_thermique_paroi': 'float',
               'coefficient_transmission_thermique_paroi_non_isolee': 'float',
               'tv003_umur_id': 'category',
               'tv004_umur0_id': 'category',
               'tv005_upb_id': 'category',
               'tv006_upb0_id': 'category',
               'tv007_uph_id': 'category',
               'tv008_uph0_id': 'category',
               'resistance_thermique_isolation': 'float',
               'epaisseur_isolation': 'float',
               'surface_paroi': 'float'}


def merge_td007_tr_tv(td007):
    from trtvtables import DPETrTvTables
    meta = DPETrTvTables()
    table = td007.copy()
    table = meta.merge_all_tr_tables(table)  # merge all tr tables inside the table
    table = meta.merge_all_tv_tables(table)
    table = table.astype({k: v for k, v in td007_types.items() if k in table})
    table = table.rename(columns={'id': 'td007_paroi_opaque_id'})

    return table


def postprocessing_td007(td007, td008):
    table = td007.copy()

    # calcul matériau
    table['materiaux_structure'] = np.nan
    is_tv004 = ~table['tv004_materiaux'].isnull()

    table.loc[is_tv004, 'materiaux_structure'] = table.loc[is_tv004, 'tv004_materiaux']

    is_tv006 = ~table['tv006_materiaux'].isnull()

    table.loc[is_tv006, 'materiaux_structure'] = table.loc[is_tv006, 'tv006_materiaux']

    is_tv008 = ~table['tv008_materiaux'].isnull()

    table.loc[is_tv008, 'materiaux_structure'] = table.loc[is_tv008, 'tv008_materiaux']

    # calcul isolation

    null = table['coefficient_transmission_thermique_paroi_non_isolee'] == 0
    table.loc[null, 'coefficient_transmission_thermique_paroi_non_isolee'] = np.nan
    null = table['coefficient_transmission_thermique_paroi'] == 0
    table.loc[null, 'coefficient_transmission_thermique_paroi'] = np.nan

    # calculs paroi opaque
    table['is_custom_resistance_thermique_isolation'] = table.resistance_thermique_isolation > 0
    table['is_custom_epaisseur_isolation'] = table.epaisseur_isolation > 0
    table[
        'resistance_thermique_isolation_calc'] = 1 / table.coefficient_transmission_thermique_paroi - 1 / table.coefficient_transmission_thermique_paroi_non_isolee
    is_null = (table.coefficient_transmission_thermique_paroi == 0) | (
            table.coefficient_transmission_thermique_paroi_non_isolee == 0)
    table.loc[is_null, 'resistance_thermique_isolation_calc'] = np.nan
    u_paroi_2 = table.coefficient_transmission_thermique_paroi > 1.95

    table.loc[u_paroi_2, 'resistance_thermique_isolation_calc'] = 0
    res_neg = table.resistance_thermique_isolation_calc < 0
    table.loc[res_neg, 'resistance_thermique_isolation_calc'] = 0
    is_plancher = table.tr014_type_parois_opaque_id == '3'
    table.loc[is_plancher, 'epaisseur_isolation_calc'] = 4.2 * table.loc[
        is_plancher, 'resistance_thermique_isolation_calc']
    table.loc[~is_plancher, 'epaisseur_isolation_calc'] = 4 * table.loc[
        ~is_plancher, 'resistance_thermique_isolation_calc']

    table.loc[table.is_custom_epaisseur_isolation, 'epaisseur_isolation_glob'] = table.loc[
        table.is_custom_epaisseur_isolation, 'epaisseur_isolation']
    table.loc[~table.is_custom_epaisseur_isolation, 'epaisseur_isolation_glob'] = table.loc[
        ~table.is_custom_epaisseur_isolation, 'epaisseur_isolation_calc']

    table.loc[table.is_custom_resistance_thermique_isolation, 'resistance_thermique_isolation_glob'] = table.loc[
        table.is_custom_resistance_thermique_isolation, 'resistance_thermique_isolation']
    table.loc[~table.is_custom_resistance_thermique_isolation, 'resistance_thermique_isolation_glob'] = table.loc[
        ~table.is_custom_resistance_thermique_isolation, 'resistance_thermique_isolation_calc']

    table.loc[table.is_custom_epaisseur_isolation, 'epaisseur_isolation_glob'] = table.loc[
        table.is_custom_epaisseur_isolation, 'epaisseur_isolation']
    table.loc[~table.is_custom_epaisseur_isolation, 'epaisseur_isolation_glob'] = table.loc[
        ~table.is_custom_epaisseur_isolation, 'epaisseur_isolation_calc']

    table.loc[table.is_custom_resistance_thermique_isolation, 'resistance_thermique_isolation_glob'] = table.loc[
        table.is_custom_resistance_thermique_isolation, 'resistance_thermique_isolation']
    table.loc[~table.is_custom_resistance_thermique_isolation, 'resistance_thermique_isolation_glob'] = table.loc[
        ~table.is_custom_resistance_thermique_isolation, 'resistance_thermique_isolation_calc']

    tv_col_isole = [col for col in table.columns.sort_values() if col.endswith('_isole')]
    # we consider an insulated paroi if it has more than 5cm of insulation.
    table['is_paroi_isole'] = (table.epaisseur_isolation_glob > 5) | (
            table.coefficient_transmission_thermique_paroi < 0.6)  # equivalent U = 0.6-> 5cm d'isolation lambda = 0.04
    # TODO : distinguer terre plein
    is_tv_isole = table[tv_col_isole].isin(['Oui', '1']).sum(axis=1) > 0

    table['is_paroi_isole'] = table['is_paroi_isole'] | is_tv_isole

    is_terre_plein = table[tv_col_isole].isin(['Terre Plein']).sum(axis=1) > 0

    table.loc[is_terre_plein, 'is_paroi_isole'] = 'Terre Plein'

    table.is_paroi_isole = table.is_paroi_isole.replace({False: 'Non', True: 'Oui'})
    table = calc_surface_paroi_opaque(table, td008)

    return table




def calc_surface_paroi_opaque(td007, td008):
    # calcul des surfaces parois_opaque + paroi vitrée

    def calc_surf_approx_equality(s1, s2, rtol=0.05, atol=2):
        is_close = np.isclose(s1, s2, rtol=rtol)
        is_close = is_close | np.isclose(s1, s2, atol=atol)
        return is_close

    surf = td008.groupby('td007_paroi_opaque_id')[['surface', 'surfacexnb_baie_calc', 'nb_baie_calc']].sum()
    surf.columns = ['surface_baie_sum', 'surfacexnb_baie_calc_sum', 'nb_baie_calc']
    surf['surfacexnb_baie_calc_sum'] = surf.max(axis=1)

    td007_m = td007.merge(surf, on='td007_paroi_opaque_id', how='left')
    td007_m['surface_paroi_opaque_calc'] = td007_m.deperdition_thermique / (
            td007_m.coefficient_transmission_thermique_paroi.astype(float) * td007_m.tv001_valeur.astype(float))

    td007_m['surface_paroi_totale_calc_v1'] = td007_m.surface_paroi_opaque_calc + td007_m.surfacexnb_baie_calc_sum
    td007_m['surface_paroi_totale_calc_v2'] = td007_m.surface_paroi_opaque_calc + td007_m.surface_baie_sum

    is_surface_totale_v1 = calc_surf_approx_equality(td007_m.surface_paroi, td007_m.surface_paroi_totale_calc_v1)
    is_surface_totale_v2 = calc_surf_approx_equality(td007_m.surface_paroi, td007_m.surface_paroi_totale_calc_v2)
    is_surface_paroi_opaque = calc_surf_approx_equality(td007_m.surface_paroi, td007_m.surface_paroi_opaque_calc)
    is_surface_paroi_opaque_deg = calc_surf_approx_equality(td007_m.surface_paroi, td007_m.surface_paroi_opaque_calc,
                                                            rtol=0.1)

    td007_m['qualif_surf'] = 'NONDEF'
    td007_m.loc[is_surface_paroi_opaque_deg, 'qualif_surf'] = 'surface_paroi=surface_paroi_opaque'
    td007_m.loc[
        is_surface_totale_v1, 'qualif_surf'] = 'surface_paroi=surface_paroi_opaque+somme(surface baiesxnb_baies) v1'
    td007_m.loc[is_surface_totale_v2, 'qualif_surf'] = 'surface_paroi=surface_paroi_opaque+somme(surface baies) v2'
    td007_m.loc[is_surface_paroi_opaque, 'qualif_surf'] = 'surface_paroi=surface_paroi_opaque'
    td007_m.loc[is_surface_paroi_opaque, 'qualif_surf'] = 'surface_paroi=surface_paroi_opaque'
    td007_m.qualif_surf = td007_m.qualif_surf.astype('category')

    td007_m['surface_paroi_totale_calc_v1'] = td007_m.surface_paroi_opaque_calc + td007_m.surfacexnb_baie_calc_sum
    td007_m['surface_paroi_totale_calc_v2'] = td007_m.surface_paroi_opaque_calc + td007_m.surface_baie_sum

    # infer surface paroi opaque

    td007_m['surface_paroi_opaque_infer'] = np.nan

    is_surface_seul = td007_m.qualif_surf == 'surface_paroi=surface_paroi_opaque'
    td007_m.loc[is_surface_seul, 'surface_paroi_opaque_infer'] = td007_m.surface_paroi

    is_surface_sum_baie_opaque_v1 = td007_m.qualif_surf == 'surface_paroi=surface_paroi_opaque+somme(surface baiesxnb_baies) v1'
    td007_m.loc[
        is_surface_sum_baie_opaque_v1, 'surface_paroi_opaque_infer'] = td007_m.surface_paroi - td007_m.surfacexnb_baie_calc_sum

    is_surface_sum_baie_opaque_v2 = td007_m.qualif_surf == 'surface_paroi=surface_paroi_opaque+somme(surface baies) v2'

    td007_m.loc[
        is_surface_sum_baie_opaque_v2, 'surface_paroi_opaque_infer'] = td007_m.surface_paroi - td007_m.surface_baie_sum

    null = td007_m.surface_paroi_opaque_infer.isnull()
    td007_m.loc[null, 'surface_paroi_opaque_infer'] = td007_m.loc[null, 'surface_paroi']

    # infer surface paroi opaque deperditive

    td007_m['surface_paroi_opaque_deperditive_infer'] = td007_m.surface_paroi_opaque_infer

    is_not_deper = (td007_m.deperdition_thermique == 0) | (td007_m.tv001_valeur == 0)
    td007_m.loc[is_not_deper, 'surface_paroi_opaque_deperditive_infer'] = np.nan

    td007_m['b_infer'] = td007_m.deperdition_thermique / (
            td007_m.surface_paroi * td007_m.coefficient_transmission_thermique_paroi)

    # infer surface paroi opaque exterieure

    td007_m['surface_paroi_opaque_exterieur_infer'] = td007_m.surface_paroi_opaque_infer
    is_tv002 = td007_m.tv002_local_non_chauffe_id.isnull() == False
    is_tv001_non_ext = td007_m.tv001_coefficient_reduction_deperditions_id != '1'
    is_non_ext_from_b_infer = td007_m.b_infer.round(2) < 0.96
    is_non_ext = (is_tv002) | (is_tv001_non_ext) | (is_non_ext_from_b_infer)
    td007_m.loc[is_non_ext, 'surface_paroi_opaque_exterieur_infer'] = np.nan

    return td007_m


def agg_surface_envelope(td007, td008):
    # SURFACES

    td008_porte = td008.loc[td008.cat_baie_simple_infer == 'porte']

    td008_vitree = td008.loc[td008.cat_baie_simple_infer.isin(['baie_vitree',
                                                               'brique_ou_poly'])]
    surf_vitree = td008_vitree.groupby('td001_dpe_id')['surfacexnb_baie_calc'].sum()
    surf_vitree.name = 'surface_vitree_totale'
    surf_porte = td008_porte.groupby('td001_dpe_id')['surfacexnb_baie_calc'].sum()
    surf_porte.name = 'surface_porte_totale'

    surf_vitree_orient = td008_vitree.pivot_table(index='td001_dpe_id', columns='orientation_infer',
                                                  values='surfacexnb_baie_calc', aggfunc='sum')

    surf_vitree_orient.columns = [f'surfaces_vitree_orientee_{el.lower().replace(" ", "_")}' for el in
                                  surf_vitree_orient.columns]

    td007_murs = td007.loc[td007.tr014_type_parois_opaque_id.isin(['2', '1'])]
    td007_pb = td007.loc[td007.tr014_type_parois_opaque_id == '3']
    td007_ph = td007.loc[td007.tr014_type_parois_opaque_id == '4']

    surf_mur = td007_murs.groupby('td001_dpe_id')[['surface_paroi_opaque_infer',
                                                   'surface_paroi_opaque_deperditive_infer',
                                                   'surface_paroi_opaque_exterieur_infer']].sum()
    surf_pb = td007_pb.groupby('td001_dpe_id')[['surface_paroi_opaque_infer',
                                                'surface_paroi_opaque_deperditive_infer']].sum()

    surf_ph = td007_ph.groupby('td001_dpe_id')[['surface_paroi_opaque_infer',
                                                'surface_paroi_opaque_deperditive_infer']].sum()

    surf_mur.columns = ['surf_murs_totale', 'surf_murs_deper', 'surf_murs_ext']
    surf_pb.columns = ['surf_pb_totale', 'surf_pb_deper']
    surf_ph.columns = ['surf_ph_totale', 'surf_ph_deper']

    quantitatif = pd.concat([surf_mur, surf_pb, surf_ph, surf_vitree, surf_porte, surf_vitree_orient], axis=1)
    quantitatif[quantitatif < 0] = np.nan
    # TODO : changer ratio -> percentage (mauvaise def)
    quantitatif['ratio_surface_vitree_exterieur'] = quantitatif.surface_vitree_totale / (
            quantitatif.surf_murs_ext + quantitatif.surface_vitree_totale)
    is_not_surf_ext = quantitatif.surf_murs_ext == 0
    quantitatif.loc[is_not_surf_ext, 'ratio_surface_vitree_exterieur'] = np.nan
    quantitatif['ratio_surface_vitree_deperditif'] = quantitatif.surface_vitree_totale / (
            quantitatif.surf_murs_deper + quantitatif.surface_vitree_totale)
    is_not_surf_deper = quantitatif.surf_murs_deper == 0
    quantitatif.loc[is_not_surf_deper, 'ratio_surface_vitree_deperditif'] = np.nan
    quantitatif['ratio_surface_vitree_total'] = quantitatif.surface_vitree_totale / (
            quantitatif.surf_murs_totale + quantitatif.surface_vitree_totale)
    for ratio_surface_vitree_col in ['ratio_surface_vitree_exterieur',
                                     'ratio_surface_vitree_deperditif',
                                     'ratio_surface_vitree_total']:
        ratio_surface_vitree = quantitatif[ratio_surface_vitree_col]
        anomaly_ratio_surface_vitree = ratio_surface_vitree > 0.95
        quantitatif.loc[anomaly_ratio_surface_vitree, ratio_surface_vitree_col] = np.nan

    return quantitatif


def agg_td007_to_td001_essential(td007):
    td007_murs = td007.loc[td007.tr014_type_parois_opaque_id.isin(['2', '1'])]
    td007_pb = td007.loc[td007.tr014_type_parois_opaque_id == '3']
    td007_ph = td007.loc[td007.tr014_type_parois_opaque_id == '4']

    Umurs_ext_avg = agg_pond_avg(td007_murs, 'coefficient_transmission_thermique_paroi',
                                 'surface_paroi_opaque_exterieur_infer',
                                 'td001_dpe_id').to_frame('Umurs_ext_avg')

    Umurs_avg = agg_pond_avg(td007_murs, 'coefficient_transmission_thermique_paroi',
                             'surface_paroi_opaque_deperditive_infer',
                             'td001_dpe_id').to_frame('Umurs_deper_avg')

    Uplancher_avg = agg_pond_avg(td007_pb, 'coefficient_transmission_thermique_paroi',
                                 'surface_paroi_opaque_deperditive_infer',
                                 'td001_dpe_id').to_frame('Uplancher_bas_deper_avg')

    Uplafond_avg = agg_pond_avg(td007_ph, 'coefficient_transmission_thermique_paroi',
                                'surface_paroi_opaque_deperditive_infer',
                                'td001_dpe_id').to_frame('Uplancher_haut_deper_avg')

    # is_pb_isole = agg_pond_top_freq(td007_pb, 'is_paroi_isole', 'surface_paroi_opaque_deperditive_infer',
    #                                 'td001_dpe_id').to_frame('is_plancher_bas_deper_isole')
    #
    # is_ph_isole = agg_pond_top_freq(td007_ph, 'is_paroi_isole', 'surface_paroi_opaque_deperditive_infer',
    #                                 'td001_dpe_id').to_frame('is_plancher_haut_deper_isole')
    #
    # is_mext_isole = agg_pond_top_freq(td007_murs, 'is_paroi_isole', 'surface_paroi_opaque_exterieur_infer',
    #                                   'td001_dpe_id').to_frame('is_murs_ext_isole')
    #
    # is_mdeper_isole = agg_pond_top_freq(td007_murs, 'is_paroi_isole', 'surface_paroi_opaque_deperditive_infer',
    #                                     'td001_dpe_id').to_frame('is_murs_deper_isole')

    mat_murs_deper_agg = agg_pond_top_freq(td007_murs, 'materiaux_structure', 'surface_paroi_opaque_deperditive_infer',
                                           'td001_dpe_id').to_frame('mat_murs_deper_top')

    mat_murs_ext_agg = agg_pond_top_freq(td007_murs, 'materiaux_structure', 'surface_paroi_opaque_exterieur_infer',
                                         'td001_dpe_id').to_frame('mat_murs_ext_top')

    mat_pb_agg = agg_pond_top_freq(td007_pb, 'materiaux_structure', 'surface_paroi_opaque_deperditive_infer',
                                   'td001_dpe_id').to_frame('mat_plancher_bas_deper_top')

    mat_ph_agg = agg_pond_top_freq(td007_ph, 'materiaux_structure', 'surface_paroi_opaque_deperditive_infer',
                                   'td001_dpe_id').to_frame('mat_plancher_haut_deper_top')

    agg = pd.concat([Umurs_ext_avg,
                     Umurs_avg,
                     Uplancher_avg,
                     Uplafond_avg,
                     # is_pb_isole,
                     # is_ph_isole,
                     # is_mext_isole,
                     # is_mdeper_isole,
                     mat_murs_deper_agg,
                     mat_murs_ext_agg,
                     mat_pb_agg,
                     mat_ph_agg,
                     ], axis=1)
    return agg


# ================================== TRAITEMENT DES MURS ==============================================================

def generate_murs_table(td007):
    td007_murs = td007.loc[td007.tr014_type_parois_opaque_id.isin(['2', '1'])].copy()

    float_cols = ['coefficient_transmission_thermique_paroi_non_isolee', 'coefficient_transmission_thermique_paroi',
                  'epaisseur_isolation', 'resistance_thermique_isolation']
    td007_murs[float_cols] = td007_murs[float_cols].astype(float)

    # ## label uniforme tv003

    td007_murs['tv003_periode_isolation_uniforme'] = td007_murs.tv003_annee_construction.astype('string')

    td007_murs['tv003_label_isolation_uniforme'] = td007_murs.tv003_annee_construction.astype('string')

    null = td007_murs['tv003_label_isolation_uniforme'].isnull()

    td007_murs.loc[null, 'tv003_label_isolation_uniforme'] = td007_murs.loc[null, 'tv003_annee_isolation'].astype(
        'string')

    inconnu = td007_murs.tv003_mur_isole.isnull() & (~td007_murs.tv003_annee_construction.isnull())
    non_isole = td007_murs.tv003_mur_isole == '0'
    isole = td007_murs.tv003_mur_isole == '1'
    is_annee_construction = ~td007_murs.tv003_annee_construction.isnull()
    is_annee_isolation = ~td007_murs.tv003_annee_isolation.isnull()

    td007_murs.loc[inconnu, 'tv003_label_isolation_uniforme'] = 'isol. inconnue periode constr : ' + td007_murs.loc[
        inconnu, 'tv003_label_isolation_uniforme']
    td007_murs.loc[non_isole, 'tv003_label_isolation_uniforme'] = 'non isolé'
    td007_murs.loc[isole & is_annee_construction, 'tv003_label_isolation_uniforme'] = 'isolé periode constr : ' + \
                                                                                      td007_murs.loc[
                                                                                          isole & is_annee_construction, 'tv003_label_isolation_uniforme']
    td007_murs.loc[isole & (~is_annee_construction), 'tv003_label_isolation_uniforme'] = 'isolé periode isolation :' + \
                                                                                         td007_murs.loc[isole & (
                                                                                             ~is_annee_construction), 'tv003_label_isolation_uniforme']

    # annee isolation uniforme.

    td007_murs['annee_isole_uniforme_min'] = td007_murs.tv003_annee_construction_min.astype('string')
    td007_murs['annee_isole_uniforme_max'] = td007_murs.tv003_annee_construction_max.astype('string')
    td007_murs.loc[is_annee_isolation, 'annee_isole_uniforme_min'] = td007_murs.loc[
        is_annee_isolation, 'tv003_annee_isolation_min'].astype('string')
    td007_murs.loc[is_annee_isolation, 'annee_isole_uniforme_max'] = td007_murs.loc[
        is_annee_isolation, 'tv003_annee_isolation_max'].astype('string')

    td007_murs.tv003_label_isolation_uniforme.value_counts()

    # ## label méthode calcul  U

    td007_murs['meth_calc_U'] = 'INCONNUE'

    # calc booleens
    U = td007_murs.coefficient_transmission_thermique_paroi.round(2)
    U_non_isolee = td007_murs.coefficient_transmission_thermique_paroi_non_isolee.round(2)
    bool_U_egal_0 = U.round(2) == 0.00
    bool_U_U0 = U.round(2) == U_non_isolee.round(2)
    bool_U_2 = U.round(2) >= 2 | non_isole
    bool_U_U0 = bool_U_U0 & (~bool_U_2)
    bool_U_U0_auto_isol = bool_U_U0 & (U_non_isolee < 1)
    bool_U_brut = (U <= 1) & (~bool_U_U0)
    bool_U_brut_non_isole = (U > 1) & (~bool_U_U0)
    bool_U_par_e = td007_murs.epaisseur_isolation > 0
    bool_U_par_r = td007_murs.resistance_thermique_isolation > 0

    # remplacer 0 par nan lorsque les 0 sont des non information.

    td007_murs.loc[~bool_U_par_e, 'epaisseur_isolation'] = np.nan
    td007_murs.loc[~bool_U_par_r, 'resistance_thermique_isolation'] = np.nan

    # imputation labels

    td007_murs.loc[bool_U_brut, 'meth_calc_U'] = 'U SAISI DIRECTEMENT : ISOLE'
    td007_murs.loc[bool_U_brut_non_isole, 'meth_calc_U'] = 'U SAISI DIRECTEMENT : NON ISOLE'
    td007_murs.loc[bool_U_par_e, 'meth_calc_U'] = 'EPAISSEUR ISOLATION SAISIE'
    td007_murs.loc[bool_U_par_r, 'meth_calc_U'] = 'RESISTANCE ISOLATION SAISIE'
    td007_murs.loc[bool_U_2, 'meth_calc_U'] = 'MUR NON ISOLE U=2'
    td007_murs.loc[bool_U_U0, 'meth_calc_U'] = 'MUR NON ISOLE U<2'
    td007_murs.loc[bool_U_U0_auto_isol, 'meth_calc_U'] = 'STRUCTURE ISOLANTE (ITR) U<1'
    td007_murs.loc[inconnu, 'meth_calc_U'] = 'PAR DEFAUT PERIODE : ISOLATION INCONNUE'
    td007_murs.loc[isole, 'meth_calc_U'] = 'PAR DEFAUT PERIODE : ISOLE'
    td007_murs.loc[isole, 'meth_calc_U'] = 'PAR DEFAUT PERIODE : ISOLE'
    td007_murs.loc[bool_U_egal_0, 'meth_calc_U'] = 'ERREUR : U=0'

    # ## label isolatoin

    td007_murs['isolation'] = 'NON ISOLE'
    is_isole = ~td007_murs.meth_calc_U.str.contains('NON ISOLE|INCONNUE')
    td007_murs.loc[is_isole, 'isolation'] = 'ISOLE SAISI'
    is_isole_defaut = is_isole & (td007_murs.meth_calc_U.str.contains('DEFAUT'))
    td007_murs.loc[is_isole_defaut, 'isolation'] = 'ISOLE DEFAUT PRE 1988'

    inconnu = td007_murs.meth_calc_U.str.contains('INCONNUE')
    post_88 = td007_murs['annee_isole_uniforme_min'] >= "1988"

    td007_murs.loc[inconnu, 'isolation'] = 'ISOLATION INCONNUE (DEFAUT)'

    td007_murs.loc[(inconnu | is_isole_defaut) & post_88, 'isolation'] = 'ISOLE DEFAUT POST 1988'

    is_isole_struc = is_isole & (td007_murs.meth_calc_U.str.contains('STRUCTURE'))
    td007_murs.loc[is_isole_struc, 'isolation'] = 'STRUCTURE ISOLANTE (ITR)'

    is_err = td007_murs.meth_calc_U.str.contains('ERREUR')

    td007_murs.loc[is_err, 'isolation'] = 'NONDEF'

    # ## label adjacence

    td007_murs['type_adjacence'] = 'NONDEF'

    ext = td007_murs.tv001_code == 'TV001_001'

    td007_murs.loc[ext, 'type_adjacence'] = 'EXTERIEUR'

    is_dep = td007_murs.b_infer.round(1) >= 0.9

    td007_murs.loc[is_dep, 'type_adjacence'] = 'EXTERIEUR'

    enterre = td007_murs.tv001_code == 'TV001_002'

    td007_murs.loc[enterre, 'type_adjacence'] = 'PAROI_ENTERREE'

    not_null = ~td007_murs.tv002_local_non_chauffe.isnull()

    td007_murs.loc[not_null, 'type_adjacence'] = 'LNC'

    is_lnc = td007_murs.tv001_code.astype('string') > 'TV001_004'

    td007_murs.loc[is_lnc, 'type_adjacence'] = 'LNC'

    is_adj = td007_murs.tv001_code == 'TV001_004'

    td007_murs.loc[is_adj, 'type_adjacence'] = 'BAT_ADJ'

    # TODO :tv001_262 ???

    return td007_murs


def agg_td007_murs_to_td001(td007_murs):
    td007_murs = td007_murs.rename(columns={'tv004_epaisseur': 'epaisseur_structure',
                                            'tv002_local_non_chauffe': 'type_local_non_chauffe',
                                            'coefficient_transmission_thermique_paroi': 'U'})

    concat = list()
    type_adjacence_top = agg_pond_top_freq(td007_murs, 'type_adjacence', 'surface_paroi_opaque_infer',
                                           'td001_dpe_id').to_frame(f'type_adjacence_top')

    type_adjacence_arr_agg = td007_murs.groupby('td001_dpe_id').type_adjacence.agg(
        lambda x: np.sort(x.dropna().unique()).tolist())

    type_adjacence_arr_agg.name = 'type_adjacence_array'

    concat.append(type_adjacence_top)
    concat.append(type_adjacence_arr_agg)

    type_local_non_chauffe_arr_agg = td007_murs.groupby('td001_dpe_id').type_local_non_chauffe.agg(
        lambda x: np.sort(x.dropna().unique()).tolist())
    type_local_non_chauffe_arr_agg = type_local_non_chauffe_arr_agg.to_frame('type_LNC_murs_array')
    type_local_non_chauffe_agg_top = agg_pond_top_freq(td007_murs, 'type_local_non_chauffe',
                                                       'surface_paroi_opaque_infer',
                                                       'td001_dpe_id').to_frame(f'type_LNC_murs_top')

    pivot = td007_murs.pivot_table(index='td001_dpe_id', columns='type_adjacence', values='surface_paroi_opaque_infer',
                                   aggfunc='sum')
    pivot.columns = [f'surface_murs_{col.lower()}' for col in pivot]
    concat.extend([type_local_non_chauffe_arr_agg, type_local_non_chauffe_agg_top, pivot])

    for var in ['meth_calc_U', 'U', 'epaisseur_isolation', 'resistance_thermique_isolation', 'isolation',
                'annee_isole_uniforme_min', 'annee_isole_uniforme_max', 'materiaux_structure', 'epaisseur_structure',
                ]:
        var_agg = agg_pond_top_freq(td007_murs, var, 'surface_paroi_opaque_infer',
                                    'td001_dpe_id').to_frame(f'{var}_murs_top')
        concat.append(var_agg)

    for type_adjacence in ['EXTERIEUR', 'LNC', 'BAT_ADJ']:
        sel = td007_murs.loc[td007_murs.type_adjacence == type_adjacence]
        for var in ['meth_calc_U', 'U', 'epaisseur_isolation', 'resistance_thermique_isolation', 'isolation',
                    'annee_isole_uniforme_min', 'annee_isole_uniforme_max', 'materiaux_structure',
                    'epaisseur_structure',
                    ]:
            var_agg = agg_pond_top_freq(sel, var, 'surface_paroi_opaque_infer',
                                        'td001_dpe_id').to_frame(f'{var}_murs_{type_adjacence.lower()}_top')
            concat.append(var_agg)

    td007_murs_agg = pd.concat(concat, axis=1)

    td007_murs_agg.index.name = 'td001_dpe_id'

    return td007_murs_agg


# ================================== TRAITEMENT DES PLANCHERS BAS ==============================================================


def generate_pb_table(td007):
    td007_pb = td007.loc[td007.tr014_type_parois_opaque_id == '3'].copy()

    float_cols = ['coefficient_transmission_thermique_paroi_non_isolee', 'coefficient_transmission_thermique_paroi',
                  'epaisseur_isolation', 'resistance_thermique_isolation']
    td007_pb[float_cols] = td007_pb[float_cols].astype(float)

    # ## label uniforme tv005

    td007_pb['tv005_periode_isolation_uniforme'] = td007_pb.tv005_annee_construction.astype('string')

    td007_pb['tv005_label_isolation_uniforme'] = td007_pb.tv005_annee_construction.astype('string')

    null = td007_pb['tv005_label_isolation_uniforme'].isnull()

    td007_pb.loc[null, 'tv005_label_isolation_uniforme'] = td007_pb.loc[null, 'tv005_annee_isolation'].astype(
        'string')

    inconnu = td007_pb.tv005_pb_isole == "Inconnu"
    non_isole = td007_pb.tv005_pb_isole == 'Non'
    isole = td007_pb.tv005_pb_isole == '1'
    tp = td007_pb.tv005_pb_isole == 'Terre Plein'

    is_annee_construction = ~td007_pb.tv005_annee_construction.isnull()
    is_annee_isolation = ~td007_pb.tv005_annee_isolation.isnull()

    td007_pb.loc[inconnu, 'tv005_label_isolation_uniforme'] = 'isol. inconnue periode constr : ' + td007_pb.loc[
        inconnu, 'tv005_label_isolation_uniforme']
    td007_pb.loc[non_isole, 'tv005_label_isolation_uniforme'] = 'non isolé'

    td007_pb.loc[isole & is_annee_construction, 'tv005_label_isolation_uniforme'] = 'isolé periode constr : ' + \
                                                                                    td007_pb.loc[
                                                                                        isole & is_annee_construction, 'tv005_label_isolation_uniforme']
    td007_pb.loc[isole & (~is_annee_construction), 'tv005_label_isolation_uniforme'] = 'isolé periode isolation :' + \
                                                                                       td007_pb.loc[isole & (
                                                                                           ~is_annee_construction), 'tv005_label_isolation_uniforme']

    td007_pb.loc[isole & (~is_annee_construction), 'tv005_label_isolation_uniforme'] = 'isolé periode isolation :' + \
                                                                                       td007_pb.loc[isole & (
                                                                                           ~is_annee_construction), 'tv005_label_isolation_uniforme']

    td007_pb.loc[tp, 'tv005_label_isolation_uniforme'] = 'Terre Plein periode constr : ' + td007_pb.loc[
        tp, 'tv005_label_isolation_uniforme']

    # annee isolation uniforme.

    td007_pb['annee_isole_uniforme_min'] = td007_pb.tv005_annee_construction_min.astype('string')
    td007_pb['annee_isole_uniforme_max'] = td007_pb.tv005_annee_construction_max.astype('string')
    td007_pb.loc[is_annee_isolation, 'annee_isole_uniforme_min'] = td007_pb.loc[
        is_annee_isolation, 'tv005_annee_isolation_min'].astype('string')
    td007_pb.loc[is_annee_isolation, 'annee_isole_uniforme_max'] = td007_pb.loc[
        is_annee_isolation, 'tv005_annee_isolation_max'].astype('string')

    # ## label méthode calcul  U

    td007_pb['meth_calc_U'] = 'INCONNUE'

    # calc booleens
    U = td007_pb.coefficient_transmission_thermique_paroi.round(2)
    U_non_isolee = td007_pb.coefficient_transmission_thermique_paroi_non_isolee.round(2)
    bool_U_egal_0 = U.round(2) == 0.00
    bool_U_U0 = U.round(2) == U_non_isolee.round(2)
    bool_U_2 = U.round(2) >= 2 | non_isole
    bool_U_U0 = bool_U_U0 & (~bool_U_2)
    bool_U_U0_auto_isol = bool_U_U0 & (U_non_isolee < 1)
    bool_U_brut = (U <= 1) & (~bool_U_U0)
    bool_U_brut_non_isole = (U > 1) & (~bool_U_U0)
    bool_U_par_e = td007_pb.epaisseur_isolation > 0
    bool_U_par_r = td007_pb.resistance_thermique_isolation > 0

    # remplacer 0 par nan lorsque les 0 sont des non information.

    td007_pb.loc[~bool_U_par_e, 'epaisseur_isolation'] = np.nan
    td007_pb.loc[~bool_U_par_r, 'resistance_thermique_isolation'] = np.nan

    # imputation labels

    td007_pb.loc[bool_U_brut, 'meth_calc_U'] = 'U SAISI DIRECTEMENT : ISOLE'
    td007_pb.loc[bool_U_brut_non_isole, 'meth_calc_U'] = 'U SAISI DIRECTEMENT : NON ISOLE'
    td007_pb.loc[bool_U_par_e, 'meth_calc_U'] = 'EPAISSEUR ISOLATION SAISIE'
    td007_pb.loc[bool_U_par_r, 'meth_calc_U'] = 'RESISTANCE ISOLATION SAISIE'
    td007_pb.loc[bool_U_2, 'meth_calc_U'] = 'PLANCHER NON ISOLE U=2'
    td007_pb.loc[bool_U_U0, 'meth_calc_U'] = 'PLANCHER NON ISOLE U<2'
    td007_pb.loc[bool_U_U0_auto_isol, 'meth_calc_U'] = 'STRUCTURE ISOLANTE U<1'
    td007_pb.loc[inconnu, 'meth_calc_U'] = 'PAR DEFAUT PERIODE : ISOLATION INCONNUE'
    td007_pb.loc[isole, 'meth_calc_U'] = 'PAR DEFAUT PERIODE : ISOLE'
    td007_pb.loc[tp, 'meth_calc_U'] = 'PAR DEFAUT PERIODE : TERRE PLEIN'
    td007_pb.loc[bool_U_egal_0, 'meth_calc_U'] = 'ERREUR : U=0'

    # ## label isolatoin

    td007_pb['isolation'] = 'NON ISOLE'
    is_isole = ~td007_pb.meth_calc_U.str.contains('NON ISOLE|INCONNUE|TERRE')
    td007_pb.loc[is_isole, 'isolation'] = 'ISOLE SAISI'
    is_isole_defaut = is_isole & (td007_pb.meth_calc_U.str.contains('DEFAUT'))
    td007_pb.loc[is_isole_defaut, 'isolation'] = 'ISOLE DEFAUT PRE 1982'

    inconnu = td007_pb.meth_calc_U.str.contains('INCONNUE')
    post_82 = td007_pb['annee_isole_uniforme_min'] >= "1982"
    post_2001 = td007_pb['annee_isole_uniforme_min'] >= "2001"

    td007_pb.loc[inconnu, 'isolation'] = 'ISOLATION INCONNUE (DEFAUT)'

    td007_pb.loc[(inconnu | is_isole_defaut) & post_82, 'isolation'] = 'ISOLE DEFAUT POST 1982'

    td007_pb.loc[tp, 'isolation'] = 'TERRE PLEIN DEFAUT PRE 2001'
    td007_pb.loc[tp & post_2001, 'isolation'] = 'TERRE PLEIN DEFAUT POST 2001'

    is_isole_struc = is_isole & (td007_pb.meth_calc_U.str.contains('STRUCTURE'))

    td007_pb.loc[is_isole_struc, 'isolation'] = 'STRUCTURE ISOLANTE'

    is_err = td007_pb.meth_calc_U.str.contains('ERREUR')

    td007_pb.loc[is_err, 'isolation'] = 'NONDEF'

    # ## label adjacence

    td007_pb['type_adjacence'] = 'NONDEF'

    ext = td007_pb.tv001_code == 'TV001_001'

    td007_pb.loc[ext, 'type_adjacence'] = 'EXTERIEUR'

    is_dep = td007_pb.b_infer.round(1) >= 0.9

    td007_pb.loc[is_dep, 'type_adjacence'] = 'EXTERIEUR'

    enterre = td007_pb.tv001_code == 'TV001_002'

    td007_pb.loc[enterre, 'type_adjacence'] = 'PAROI_ENTERREE'

    not_null = ~td007_pb.tv002_local_non_chauffe.isnull()

    td007_pb.loc[not_null, 'type_adjacence'] = 'LNC'

    is_lnc = td007_pb.tv001_code.astype('string') > 'TV001_004'

    td007_pb.loc[is_lnc, 'type_adjacence'] = 'LNC'

    is_adj = td007_pb.tv001_code == 'TV001_004'

    td007_pb.loc[is_adj, 'type_adjacence'] = 'BAT_ADJ'

    is_tp = td007_pb.tv001_code == 'TV001_261'

    td007_pb.loc[is_tp, 'type_adjacence'] = 'TERRE_PLEIN'

    is_vs = td007_pb.tv001_code == 'TV001_003'

    td007_pb.loc[is_vs, 'type_adjacence'] = 'VIDE_SANITAIRE'

    td007_pb.type_adjacence_simple = td007_pb.type_adjacence.replace({'TERRE_PLEIN': 'TP_VS',
                                                                      'VIDE_SANITAIRE': 'TP_VS'})
    return td007_pb


def agg_td007_pb_to_td001(td007_pb):
    td007_pb = td007_pb.rename(columns={
        'tv002_local_non_chauffe': 'type_local_non_chauffe',
        'coefficient_transmission_thermique_paroi': 'U'})
    concat = list()
    type_adjacence_top = agg_pond_top_freq(td007_pb, 'type_adjacence', 'surface_paroi_opaque_infer',
                                           'td001_dpe_id').to_frame(f'type_adjacence_top')

    type_adjacence_arr_agg = td007_pb.groupby('td001_dpe_id').type_adjacence.agg(
        lambda x: np.sort(x.dropna().unique()).tolist())

    type_adjacence_arr_agg.name = 'type_adjacence_array'

    concat.append(type_adjacence_top)
    concat.append(type_adjacence_arr_agg)

    type_local_non_chauffe_arr_agg = td007_pb.groupby('td001_dpe_id').type_local_non_chauffe.agg(
        lambda x: np.sort(x.dropna().unique()).tolist())
    type_local_non_chauffe_arr_agg = type_local_non_chauffe_arr_agg.to_frame('type_LNC_planchers_array')
    type_local_non_chauffe = agg_pond_top_freq(td007_pb, 'type_local_non_chauffe', 'surface_paroi_opaque_infer',
                                               'td001_dpe_id').to_frame(f'type_LNC_planchers_top')

    pivot = td007_pb.pivot_table(index='td001_dpe_id', columns='type_adjacence', values='surface_paroi_opaque_infer',
                                 aggfunc='sum')
    pivot.columns = [f'surface_planchers_{col.lower()}' for col in pivot]

    concat.extend([type_local_non_chauffe_arr_agg, type_local_non_chauffe_agg_top, pivot])

    for var in ['meth_calc_U', 'U', 'epaisseur_isolation', 'resistance_thermique_isolation', 'isolation',
                'annee_isole_uniforme_min', 'annee_isole_uniforme_max', 'materiaux_structure',
                ]:
        var_agg = agg_pond_top_freq(td007_pb, var, 'surface_paroi_opaque_infer',
                                    'td001_dpe_id').to_frame(f'{var}_plancher_top')
        concat.append(var_agg)

    for type_adjacence_simple in ['EXTERIEUR', 'TP_VS', 'LNC', 'BAT_ADJ']:
        sel = td007_pb.loc[td007_pb.type_adjacence_simple == type_adjacence_simple]
        for var in ['meth_calc_U', 'U', 'epaisseur_isolation', 'resistance_thermique_isolation', 'isolation',
                    'annee_isole_uniforme_min', 'annee_isole_uniforme_max', 'materiaux_structure',
                    ]:
            var_agg = agg_pond_top_freq(sel, var, 'surface_paroi_opaque_infer',
                                        'td001_dpe_id').to_frame(f'{var}_plancher_{type_adjacence_simple.lower()}_top')
            concat.append(var_agg)

    td007_pb_agg = pd.concat(concat, axis=1)

    td007_pb_agg.index.name = 'td001_dpe_id'

    return td007_pb_agg


# ================================== TRAITEMENT DES PLANCHERS HAUTS ==============================================================


def generate_ph_table(td007):
    td007_pb = td007.loc[td007.tr014_type_parois_opaque_id == '3'].copy()

    float_cols = ['coefficient_transmission_thermique_paroi_non_isolee', 'coefficient_transmission_thermique_paroi',
                  'epaisseur_isolation', 'resistance_thermique_isolation']
    td007_pb[float_cols] = td007_pb[float_cols].astype(float)

    # ## label uniforme tv005

    td007_pb['tv005_periode_isolation_uniforme'] = td007_pb.tv005_annee_construction.astype('string')

    td007_pb['tv005_label_isolation_uniforme'] = td007_pb.tv005_annee_construction.astype('string')

    null = td007_pb['tv005_label_isolation_uniforme'].isnull()

    td007_pb.loc[null, 'tv005_label_isolation_uniforme'] = td007_pb.loc[null, 'tv005_annee_isolation'].astype(
        'string')

    inconnu = td007_pb.tv005_pb_isole == "Inconnu"
    non_isole = td007_pb.tv005_pb_isole == 'Non'
    isole = td007_pb.tv005_pb_isole == '1'
    tp = td007_pb.tv005_pb_isole == 'Terre Plein'

    is_annee_construction = ~td007_pb.tv005_annee_construction.isnull()
    is_annee_isolation = ~td007_pb.tv005_annee_isolation.isnull()

    td007_pb.loc[inconnu, 'tv005_label_isolation_uniforme'] = 'isol. inconnue periode constr : ' + td007_pb.loc[
        inconnu, 'tv005_label_isolation_uniforme']
    td007_pb.loc[non_isole, 'tv005_label_isolation_uniforme'] = 'non isolé'

    td007_pb.loc[isole & is_annee_construction, 'tv005_label_isolation_uniforme'] = 'isolé periode constr : ' + \
                                                                                    td007_pb.loc[
                                                                                        isole & is_annee_construction, 'tv005_label_isolation_uniforme']
    td007_pb.loc[isole & (~is_annee_construction), 'tv005_label_isolation_uniforme'] = 'isolé periode isolation :' + \
                                                                                       td007_pb.loc[isole & (
                                                                                           ~is_annee_construction), 'tv005_label_isolation_uniforme']

    td007_pb.loc[isole & (~is_annee_construction), 'tv005_label_isolation_uniforme'] = 'isolé periode isolation :' + \
                                                                                       td007_pb.loc[isole & (
                                                                                           ~is_annee_construction), 'tv005_label_isolation_uniforme']

    td007_pb.loc[tp, 'tv005_label_isolation_uniforme'] = 'Terre Plein periode constr : ' + td007_pb.loc[
        tp, 'tv005_label_isolation_uniforme']

    # annee isolation uniforme.

    td007_pb['annee_isole_uniforme_min'] = td007_pb.tv005_annee_construction_min.astype('string')
    td007_pb['annee_isole_uniforme_max'] = td007_pb.tv005_annee_construction_max.astype('string')
    td007_pb.loc[is_annee_isolation, 'annee_isole_uniforme_min'] = td007_pb.loc[
        is_annee_isolation, 'tv005_annee_isolation_min'].astype('string')
    td007_pb.loc[is_annee_isolation, 'annee_isole_uniforme_max'] = td007_pb.loc[
        is_annee_isolation, 'tv005_annee_isolation_max'].astype('string')

    # ## label méthode calcul  U

    td007_pb['meth_calc_U'] = 'INCONNUE'

    # calc booleens
    U = td007_pb.coefficient_transmission_thermique_paroi.round(2)
    U_non_isolee = td007_pb.coefficient_transmission_thermique_paroi_non_isolee.round(2)
    bool_U_egal_0 = U.round(2) == 0.00
    bool_U_U0 = U.round(2) == U_non_isolee.round(2)
    bool_U_2 = U.round(2) >= 2 | non_isole
    bool_U_U0 = bool_U_U0 & (~bool_U_2)
    bool_U_U0_auto_isol = bool_U_U0 & (U_non_isolee < 1)
    bool_U_brut = (U <= 1) & (~bool_U_U0)
    bool_U_brut_non_isole = (U > 1) & (~bool_U_U0)
    bool_U_par_e = td007_pb.epaisseur_isolation > 0
    bool_U_par_r = td007_pb.resistance_thermique_isolation > 0

    # remplacer 0 par nan lorsque les 0 sont des non information.

    td007_pb.loc[~bool_U_par_e, 'epaisseur_isolation'] = np.nan
    td007_pb.loc[~bool_U_par_r, 'resistance_thermique_isolation'] = np.nan

    # imputation labels

    td007_pb.loc[bool_U_brut, 'meth_calc_U'] = 'U SAISI DIRECTEMENT : ISOLE'
    td007_pb.loc[bool_U_brut_non_isole, 'meth_calc_U'] = 'U SAISI DIRECTEMENT : NON ISOLE'
    td007_pb.loc[bool_U_par_e, 'meth_calc_U'] = 'EPAISSEUR ISOLATION SAISIE'
    td007_pb.loc[bool_U_par_r, 'meth_calc_U'] = 'RESISTANCE ISOLATION SAISIE'
    td007_pb.loc[bool_U_2, 'meth_calc_U'] = 'PLANCHER NON ISOLE U=2'
    td007_pb.loc[bool_U_U0, 'meth_calc_U'] = 'PLANCHER NON ISOLE U<2'
    td007_pb.loc[bool_U_U0_auto_isol, 'meth_calc_U'] = 'STRUCTURE ISOLANTE U<1'
    td007_pb.loc[inconnu, 'meth_calc_U'] = 'PAR DEFAUT PERIODE : ISOLATION INCONNUE'
    td007_pb.loc[isole, 'meth_calc_U'] = 'PAR DEFAUT PERIODE : ISOLE'
    td007_pb.loc[tp, 'meth_calc_U'] = 'PAR DEFAUT PERIODE : TERRE PLEIN'
    td007_pb.loc[bool_U_egal_0, 'meth_calc_U'] = 'ERREUR : U=0'

    # ## label isolatoin

    td007_pb['isolation'] = 'NON ISOLE'
    is_isole = ~td007_pb.meth_calc_U.str.contains('NON ISOLE|INCONNUE|TERRE')
    td007_pb.loc[is_isole, 'isolation'] = 'ISOLE SAISI'
    is_isole_defaut = is_isole & (td007_pb.meth_calc_U.str.contains('DEFAUT'))
    td007_pb.loc[is_isole_defaut, 'isolation'] = 'ISOLE DEFAUT PRE 1982'

    inconnu = td007_pb.meth_calc_U.str.contains('INCONNUE')
    post_82 = td007_pb['annee_isole_uniforme_min'] >= "1982"
    post_2001 = td007_pb['annee_isole_uniforme_min'] >= "2001"

    td007_pb.loc[inconnu, 'isolation'] = 'ISOLATION INCONNUE (DEFAUT)'

    td007_pb.loc[(inconnu | is_isole_defaut) & post_82, 'isolation'] = 'ISOLE DEFAUT POST 1982'

    td007_pb.loc[tp, 'isolation'] = 'TERRE PLEIN DEFAUT PRE 2001'
    td007_pb.loc[tp & post_2001, 'isolation'] = 'TERRE PLEIN DEFAUT POST 2001'

    is_isole_struc = is_isole & (td007_pb.meth_calc_U.str.contains('STRUCTURE'))

    td007_pb.loc[is_isole_struc, 'isolation'] = 'STRUCTURE ISOLANTE'

    is_err = td007_pb.meth_calc_U.str.contains('ERREUR')

    td007_pb.loc[is_err, 'isolation'] = 'NONDEF'

    # ## label adjacence

    td007_pb['type_adjacence'] = 'NONDEF'

    ext = td007_pb.tv001_code == 'TV001_001'

    td007_pb.loc[ext, 'type_adjacence'] = 'EXTERIEUR'

    is_dep = td007_pb.b_infer.round(1) >= 0.9

    td007_pb.loc[is_dep, 'type_adjacence'] = 'EXTERIEUR'

    enterre = td007_pb.tv001_code == 'TV001_002'

    td007_pb.loc[enterre, 'type_adjacence'] = 'PAROI_ENTERREE'

    not_null = ~td007_pb.tv002_local_non_chauffe.isnull()

    td007_pb.loc[not_null, 'type_adjacence'] = 'LNC'

    is_lnc = td007_pb.tv001_code.astype('string') > 'TV001_004'

    td007_pb.loc[is_lnc, 'type_adjacence'] = 'LNC'

    is_adj = td007_pb.tv001_code == 'TV001_004'

    td007_pb.loc[is_adj, 'type_adjacence'] = 'BAT_ADJ'

    is_tp = td007_pb.tv001_code == 'TV001_261'

    td007_pb.loc[is_tp, 'type_adjacence'] = 'TERRE_PLEIN'

    is_vs = td007_pb.tv001_code == 'TV001_003'

    td007_pb.loc[is_vs, 'type_adjacence'] = 'VIDE_SANITAIRE'

    td007_pb.type_adjacence_simple = td007_pb.type_adjacence.replace({'TERRE_PLEIN': 'TP_VS',
                                                                      'VIDE_SANITAIRE': 'TP_VS'})
    return td007_pb


def agg_td007_ph_to_td001(td007_pb):

    td007_pb = td007_pb.rename(columns={
        'tv002_local_non_chauffe': 'type_local_non_chauffe',
        'coefficient_transmission_thermique_paroi': 'U'})
    concat = list()
    type_adjacence_top = agg_pond_top_freq(td007_pb, 'type_adjacence', 'surface_paroi_opaque_infer',
                                           'td001_dpe_id').to_frame(f'type_adjacence_top')

    type_adjacence_arr_agg = td007_pb.groupby('td001_dpe_id').type_adjacence.agg(
        lambda x: np.sort(x.dropna().unique()).tolist())

    type_adjacence_arr_agg.name = 'type_adjacence_array'

    concat.append(type_adjacence_top)
    concat.append(type_adjacence_arr_agg)

    type_local_non_chauffe_arr_agg = td007_pb.groupby('td001_dpe_id').type_local_non_chauffe.agg(
        lambda x: np.sort(x.dropna().unique()).tolist())
    type_local_non_chauffe_arr_agg = type_local_non_chauffe_arr_agg.to_frame('type_LNC_planchers_array')
    type_local_non_chauffe_agg_top = agg_pond_top_freq(td007_pb, 'type_local_non_chauffe', 'surface_paroi_opaque_infer',
                                               'td001_dpe_id').to_frame(f'type_LNC_planchers_top')

    pivot = td007_pb.pivot_table(index='td001_dpe_id', columns='type_adjacence', values='surface_paroi_opaque_infer',
                                 aggfunc='sum')
    pivot.columns = [f'surface_planchers_{col.lower()}' for col in pivot]

    concat.extend([type_local_non_chauffe_arr_agg, type_local_non_chauffe_agg_top, pivot])

    for var in ['meth_calc_U', 'U', 'epaisseur_isolation', 'resistance_thermique_isolation', 'isolation',
                'annee_isole_uniforme_min', 'annee_isole_uniforme_max', 'materiaux_structure',
                ]:
        var_agg = agg_pond_top_freq(td007_pb, var, 'surface_paroi_opaque_infer',
                                    'td001_dpe_id').to_frame(f'{var}_plancher_top')
        concat.append(var_agg)

    for type_adjacence_simple in ['EXTERIEUR', 'TP_VS', 'LNC', 'BAT_ADJ']:
        sel = td007_pb.loc[td007_pb.type_adjacence_simple == type_adjacence_simple]
        for var in ['meth_calc_U', 'U', 'epaisseur_isolation', 'resistance_thermique_isolation', 'isolation',
                    'annee_isole_uniforme_min', 'annee_isole_uniforme_max', 'materiaux_structure',
                    ]:
            var_agg = agg_pond_top_freq(sel, var, 'surface_paroi_opaque_infer',
                                        'td001_dpe_id').to_frame(f'{var}_plancher_{type_adjacence_simple.lower()}_top')
            concat.append(var_agg)

    td007_pb_agg = pd.concat(concat, axis=1)

    td007_pb_agg.index.name = 'td001_dpe_id'

    return td007_pb_agg


