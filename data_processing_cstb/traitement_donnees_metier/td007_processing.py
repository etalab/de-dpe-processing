import pandas as pd
import numpy as np
from utils import agg_pond_avg,agg_pond_top_freq
td007_types = {'id': 'str',
               'td006_batiment_id': 'str',
               'tr014_type_parois_opaque_id': 'category',
               'reference': 'category',
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
               'surface_paroi': 'float',
               'tr014_Sous-Type': 'category',
               'tr014_Est_efface': 'category',
               'tv001_aiu_aue': 'category',
               'tv001_aiu_aue_min': 'category',
               'tv001_aiu_aue_max': 'category',
               'tv001_uv_ue': 'category',
               'tv001_aue_isole': 'category',
               'tv001_aiu_isole': 'category',
               'tv001_valeur': 'category',
               'tv001_est_efface': 'category',
               'tv002_Type de Bâtiment': 'category',
               'tv002_Local non chauffée': 'category',
               'tv002_Uv,ue': 'category',
               'tv003_Mur Isolé': 'category',
               'tv003_Année construction': 'category',
               'tv003_Année isolation': 'category',
               'tv003_Zone Hiver': 'category',
               'tv003_Effet Joule': 'category',
               'tv003_Umur': 'category',
               'tv004_Matériaux': 'category',
               'tv004_épaisseur': 'category',
               'tv004_Umur0': 'category',
               'tv005_Pb Isolé': 'category',
               'tv005_2S/P': 'category',
               'tv005_Année construction': 'category',
               'tv005_Année isolation': 'category',
               'tv005_Zone Hiver': 'category',
               'tv005_Effet Joule': 'category',
               'tv005_Upb': 'category',
               'tv006_Matériaux': 'category',
               'tv006_Upb0': 'category',
               'tv007_Ph Isolé': 'category',
               'tv007_Type  de Toit': 'category',
               'tv007_Année construction': 'category',
               'tv007_Année isolation': 'category',
               'tv007_Zone Hiver': 'category',
               'tv007_Effet Joule': 'category',
               'tv007_Uph': 'category',
               'tv008_Matériaux': 'category',
               'tv008_Uph0': 'category'}


def merge_td007_tr_tv(td007):
    from assets_orm import DPEMetaData
    meta = DPEMetaData()
    table = td007.copy()
    table = meta.merge_all_tr_table(table)
    table = meta.merge_all_tv_table(table)
    table = table.astype(td007_types)
    table = table.rename(columns={'id': 'td007_paroi_opaque_id'})

    return table


def postprocessing_td007(td007, td008):
    table = td007.copy()

    # calcul matériau
    table['materiaux_structure'] = 'NONDEF'
    is_tv004 = ~table['tv004_Matériaux'].isnull()

    table.loc[is_tv004, 'materiaux_structure'] = table.loc[is_tv004, 'tv004_Matériaux']

    is_tv006 = ~table['tv006_Matériaux'].isnull()

    table.loc[is_tv006, 'materiaux_structure'] = table.loc[is_tv006, 'tv006_Matériaux']

    is_tv008 = ~table['tv008_Matériaux'].isnull()

    table.loc[is_tv008, 'materiaux_structure'] = table.loc[is_tv008, 'tv008_Matériaux']

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
    is_plancher = table.tr014_type_parois_opaque_id == 'TR014_003'
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

    tv_col_isole = [col for col in table.columns.sort_values() if col.endswith(' Isolé')]
    # we consider an insulated paroi if it has more than 2cm of insulation.
    table['is_paroi_isole'] = (table.epaisseur_isolation_calc > 2) | (
            table.coefficient_transmission_thermique_paroi < 0.6)
    is_tv_isole = table[tv_col_isole].isin(['Oui', 'Terre Plein']).sum(axis=1) > 0

    table['is_paroi_isole'] = table['is_paroi_isole'] | is_tv_isole

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
    is_tv001_non_ext = td007_m.tv001_coefficient_reduction_deperditions_id != 'TV001_001'
    is_non_ext_from_b_infer = td007_m.b_infer.round(2) < 0.96
    is_non_ext = (is_tv002) | (is_tv001_non_ext) | (is_non_ext_from_b_infer)
    td007_m.loc[is_non_ext, 'surface_paroi_opaque_exterieur_infer'] = np.nan

    return td007_m

def agg_surface_envelope(td007,td008):

    # SURFACES

    td008_porte = td008.loc[td008.cat_baie_simple_infer == 'porte']

    td008_vitree = td008.loc[td008.cat_baie_simple_infer.isin(['baie vitrée',

                                                               'paroi en brique de verre ou polycarbonate'])]
    surf_vitree = td008_vitree.groupby('td001_dpe_id')['surfacexnb_baie_calc'].sum()
    surf_vitree.name = 'surface_vitree_totale'
    surf_porte = td008_porte.groupby('td001_dpe_id')['surfacexnb_baie_calc'].sum()
    surf_porte.name = 'surface_porte_totale'

    surf_vitree_orient = td008_vitree.pivot_table(index='td001_dpe_id', columns='orientation_infer',
                                                  values='surfacexnb_baie_calc', aggfunc='sum')

    surf_vitree_orient.columns = [f'surfaces_vitree_orientee_{el.lower().replace(" ", "_")}' for el in
                                  surf_vitree_orient.columns]

    td007_murs = td007.loc[td007.tr014_type_parois_opaque_id.isin(['TR014_002', 'TR014_001'])]
    td007_pb = td007.loc[td007.tr014_type_parois_opaque_id == 'TR014_003']
    td007_ph = td007.loc[td007.tr014_type_parois_opaque_id == 'TR014_004']

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
    quantitatif['ratio_surface_vitree'] = quantitatif.surface_vitree_totale / quantitatif.surf_murs_ext
    ratio_surface_vitree = quantitatif['ratio_surface_vitree']
    anomaly_ratio_surface_vitree = ratio_surface_vitree > 0.95
    quantitatif.loc[anomaly_ratio_surface_vitree, 'ratio_surface_vitree'] = np.nan
    is_not_surf_ext = quantitatif.surf_murs_ext == 0
    quantitatif.loc[is_not_surf_ext, 'ratio_surface_vitree'] = np.nan

    return quantitatif


def agg_td007_to_td001_essential(td007):
    td007_murs = td007.loc[td007.tr014_type_parois_opaque_id.isin(['TR014_002', 'TR014_001'])]
    td007_pb = td007.loc[td007.tr014_type_parois_opaque_id == 'TR014_003']
    td007_ph = td007.loc[td007.tr014_type_parois_opaque_id == 'TR014_004']

    Umurs_ext_avg = agg_pond_avg(td007_murs, 'coefficient_transmission_thermique_paroi', 'surface_paroi_opaque_exterieur_infer',
                             'td001_dpe_id').to_frame('Umurs_ext_avg')

    Umurs_avg = agg_pond_avg(td007_murs, 'coefficient_transmission_thermique_paroi', 'surface_paroi_opaque_deperditive_infer',
                             'td001_dpe_id').to_frame('Umurs_deper_avg')



    Uplancher_avg = agg_pond_avg(td007_pb, 'coefficient_transmission_thermique_paroi', 'surface_paroi_opaque_deperditive_infer',
                             'td001_dpe_id').to_frame('Uplancher_bas_deper_avg')



    Uplafond_avg = agg_pond_avg(td007_ph, 'coefficient_transmission_thermique_paroi', 'surface_paroi_opaque_deperditive_infer',
                             'td001_dpe_id').to_frame('Uplancher_haut_deper_avg')

    is_pb_isole = agg_pond_top_freq(td007_pb, 'is_paroi_isole', 'surface_paroi_opaque_deperditive_infer',
                             'td001_dpe_id').to_frame('is_plancher_bas_deper_isole')

    is_ph_isole = agg_pond_top_freq(td007_ph, 'is_paroi_isole', 'surface_paroi_opaque_deperditive_infer',
                             'td001_dpe_id').to_frame('is_plancher_haut_deper_isole')

    is_mext_isole = agg_pond_top_freq(td007_murs, 'is_paroi_isole', 'surface_paroi_opaque_exterieur_infer',
                             'td001_dpe_id').to_frame('is_murs_ext_isole')

    is_mdeper_isole = agg_pond_top_freq(td007_murs, 'is_paroi_isole', 'surface_paroi_opaque_deperditive_infer',
                             'td001_dpe_id').to_frame('is_murs_deper_isole')

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
                    is_pb_isole,
                    is_ph_isole,
                    is_mext_isole,
                    is_mdeper_isole,
                    mat_murs_deper_agg,
                    mat_murs_ext_agg,
                    mat_pb_agg,
                    mat_ph_agg,
                    ], axis=1)
    return agg
