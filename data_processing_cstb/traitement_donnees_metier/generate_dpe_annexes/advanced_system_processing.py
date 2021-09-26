import pandas as pd
import numpy as np
from generate_dpe_annexes.text_matching_dict import gen_ch_search_dict_flat, reverse_cat_gen_ch, gen_ch_search_dict, reverse_cat_gen_ecs, \
    gen_ecs_search_dict, gen_ecs_search_dict_flat, tr003_desc_to_gen, energie_combustion_mods, priorisation_ecs, \
    energie_chaudiere_mods, type_chaudiere_mods,gen_ecs_simp_dict,gen_ch_simp_dict
from generate_dpe_annexes.conversion_normalisation import energie_normalise_ordered
from generate_dpe_annexes.td003_td005_text_extraction import extract_td003_ch_variables, extract_td003_ecs_variables, \
    extract_td005_ch_variables, extract_td005_ecs_variables

from generate_dpe_annexes.td002_td016_processing import extract_type_energie_from_td002_td016, merge_td002_td016_trtrv
from generate_dpe_annexes.config import config

def main_advanced_system_processing(td001_sys_ch_agg, td001, td002, td016,
                                    td001_sys_ecs_agg, td003, td005, td011_p, td012_p, td014_p):
    td001 = td001.rename(columns={'id': 'td001_dpe_id'})
    td001_sys = td001.rename(columns={'id': 'td001_dpe_id'})[['td001_dpe_id', 'tr002_type_batiment_id']].merge(
        td001_sys_ch_agg[['td001_dpe_id',
                      'gen_ch_lib_infer_concat', 'mix_energetique_ch'
                      ]], on='td001_dpe_id',
        how='left')
    td001_sys = td001_sys.merge(td001_sys_ecs_agg[['td001_dpe_id', 'gen_ecs_lib_infer_concat', 'mix_energetique_ecs']],
                                on='td001_dpe_id',
                                how='left')
    logger = config['logger']

    # ELASTIC SEARCH descriptif et fiches techniques
    logger.debug('search CH TD005')
    gen_ch_lib_ft, type_installation_ch_ft, energie_ch_ft, solaire_ch_ft = extract_td005_ch_variables(td005)
    logger.debug('search CH TD003')

    gen_ch_lib_desc, type_installation_ch_desc, energie_ch_desc, solaire_ch_desc = extract_td003_ch_variables(td003)
    logger.debug('search ECS TD005')

    gen_ecs_lib_ft, type_installation_ecs_ft, energie_ecs_ft = extract_td005_ecs_variables(td005)
    logger.debug('search ECS TD003')

    gen_ecs_lib_desc, type_installation_ecs_desc, energie_ecs_desc = extract_td003_ecs_variables(td003)

    td001_sys = generate_lib_for_sys_ch_and_ecs(td001_sys, gen_ch_lib_ft, type_installation_ch_ft, energie_ch_ft,
                                                solaire_ch_ft,
                                                gen_ch_lib_desc, type_installation_ch_desc, energie_ch_desc,
                                                solaire_ch_desc,
                                                gen_ecs_lib_ft, type_installation_ecs_ft, energie_ecs_ft,
                                                gen_ecs_lib_desc, type_installation_ecs_desc, energie_ecs_desc
                                                , td011_p, td012_p, td014_p)

    # récuperer les energies de chauffage des tables factures et consommation.
    td002_p, td016_p = merge_td002_td016_trtrv(td002, td016)
    td001_ener_from_facture = extract_type_energie_from_td002_td016(td001, td002_p, td016_p)
    td001_sys = td001_sys.merge(td001_ener_from_facture, on='td001_dpe_id', how='left')

    # redressement métier des données (PARTI PRIS IMPORTANT)

    td001_sys = redressement_td001_sys(td001_sys)

    # simplifications des libéllés

    td001_sys = calcul_libelle_simplifie(td001_sys)

    # choix des variables de sortie.
    cols = ['td001_dpe_id',
            'gen_ch_lib_final', 'type_installation_ch', 'type_energie_ch', 'gen_ch_lib_final_simp',
            'gen_ch_lib_principal', 'gen_ch_lib_appoint',
            'gen_ch_lib_non_retraite',
            'gen_ecs_lib_non_retraite',
            'is_ch_solaire_data', 'is_ch_solaire_txt',
            'is_ch_solaire',
            'gen_ecs_lib_final', 'type_installation_ecs', 'type_energie_ecs', 'gen_ecs_lib_final_simp',
            'gen_ecs_lib_principal', 'gen_ecs_lib_appoint', 'gen_ecs_lib_final_sec',
            'gen_ecs_lib_final_defaut', 'is_ecs_solaire_txt', 'is_ecs_solaire_data',
            'is_ecs_solaire',
            'td002_type_energie_ch', 'td002_type_energie_ecs',
            'td016_type_energie_ch', 'td016_type_energie_ecs',
            'src_gen_ch_lib', 'src_gen_ecs_lib']

    return td001_sys[cols]


def generate_lib_for_sys_ch_and_ecs(td001_sys, gen_ch_lib_ft, type_installation_ch_ft, energie_ch_ft, solaire_ch_ft,
                                    gen_ch_lib_desc, type_installation_ch_desc, energie_ch_desc, solaire_ch_desc,
                                    gen_ecs_lib_ft, type_installation_ecs_ft, energie_ecs_ft,
                                    gen_ecs_lib_desc, type_installation_ecs_desc, energie_ecs_desc
                                    , td011_p, td012_p, td014_p):
    # =========================assemblage des libéllés extrait des données textes et du modèle de données =============

    gen_ch_lib = concat_and_sort_gen_ch_lib(gen_ch_lib_desc, gen_ch_lib_ft, td011_p=td011_p, td012_p=td012_p)

    type_installation_ch = concat_and_sort_type_installation_ch_lib(type_installation_ch_desc, type_installation_ch_ft,
                                                                    td011_p)
    energie_ch = concat_and_sort_energie_ch_lib(energie_ch_desc, energie_ch_ft,
                                                td012_p)

    gen_ecs_lib = concat_and_sort_gen_ecs_lib(gen_ecs_lib_desc, gen_ecs_lib_ft, td014_p=td014_p)

    type_installation_ecs = concat_and_sort_type_installation_ecs_lib(type_installation_ecs_desc,
                                                                      type_installation_ecs_ft,
                                                                      td014_p=td014_p)
    energie_ecs = concat_and_sort_energie_ecs_lib(energie_ecs_desc, energie_ecs_ft,
                                                  td014_p=td014_p)

    # ==================================amelioration des labels et suppresion des labels indetermine ==================

    # amelioration label chaudieres chauffage

    is_chaudiere_ener_ind = gen_ch_lib.label.str.contains('chaudiere energie indetermine')
    is_chaudiere_ener_ind = is_chaudiere_ener_ind & (
        ~gen_ch_lib.label.str.contains('chaudiere energie indetermine indetermine'))
    is_chaudiere_ind = gen_ch_lib.label.str.contains('chaudiere') & gen_ch_lib.label.str.contains('indetermine') & (
        ~is_chaudiere_ener_ind)
    is_chaudiere_ind = is_chaudiere_ind & (~gen_ch_lib.label.str.contains('releve'))

    gen_ch_lib['chaudiere_ener_ind'] = is_chaudiere_ener_ind
    gen_ch_lib['chaudiere_ind'] = is_chaudiere_ind

    sum_inds = gen_ch_lib.groupby('td001_dpe_id')[['chaudiere_ener_ind', 'chaudiere_ind']].sum()

    id_sel = sum_inds.loc[(sum_inds > 0).min(axis=1)].index

    ind_sel = gen_ch_lib.td001_dpe_id.isin(id_sel) & gen_ch_lib.chaudiere_ind
    chaudiere_ind = gen_ch_lib.loc[ind_sel]

    chaudiere_ener_ind = gen_ch_lib.loc[gen_ch_lib.td001_dpe_id.isin(id_sel) & gen_ch_lib.chaudiere_ener_ind]

    suffix = chaudiere_ener_ind.set_index('td001_dpe_id').label.apply(
        lambda x: x.split('energie indetermine')[-1].strip())
    suffix.name = 'suffix'

    chaudiere_ind = chaudiere_ind.merge(suffix, on='td001_dpe_id', how='left')

    chaudiere_ind.label = chaudiere_ind.label.astype(str) + ' ' + chaudiere_ind.suffix

    chaudiere_ind.label = chaudiere_ind.label.str.replace('indetermine ', '').str.strip()

    gen_ch_lib.loc[ind_sel, 'label'] = chaudiere_ind.label.values

    # amelioration labels chaudieres ecs

    is_chaudiere_ener_ind = gen_ecs_lib.label.str.contains('chaudiere energie indetermine')
    is_chaudiere_ener_ind = is_chaudiere_ener_ind & (
        ~gen_ecs_lib.label.str.contains('chaudiere energie indetermine indetermine'))
    is_chaudiere_ind = gen_ecs_lib.label.str.contains('chaudiere') & gen_ecs_lib.label.str.contains('indetermine') & (
        ~is_chaudiere_ener_ind)

    gen_ecs_lib['chaudiere_ener_ind'] = is_chaudiere_ener_ind
    gen_ecs_lib['chaudiere_ind'] = is_chaudiere_ind

    sum_inds = gen_ecs_lib.groupby('td001_dpe_id')[['chaudiere_ener_ind', 'chaudiere_ind']].sum()

    id_sel = sum_inds.loc[(sum_inds > 0).min(axis=1)].index

    ind_sel = gen_ecs_lib.td001_dpe_id.isin(id_sel) & gen_ecs_lib.chaudiere_ind
    chaudiere_ind = gen_ecs_lib.loc[ind_sel]

    chaudiere_ener_ind = gen_ecs_lib.loc[gen_ecs_lib.td001_dpe_id.isin(id_sel) & gen_ecs_lib.chaudiere_ener_ind]

    suffix = chaudiere_ener_ind.set_index('td001_dpe_id').label.apply(
        lambda x: x.split('energie indetermine')[-1].strip())
    suffix.name = 'suffix'

    chaudiere_ind = chaudiere_ind.merge(suffix, on='td001_dpe_id', how='left')

    chaudiere_ind.label = chaudiere_ind.label.astype(str) + ' ' + chaudiere_ind.suffix

    chaudiere_ind.label = chaudiere_ind.label.str.replace('indetermine ', '').str.strip()

    gen_ecs_lib.loc[ind_sel, 'label'] = chaudiere_ind.label.values

    # amelioration label pac chauffage

    is_releve_chaudiere = gen_ch_lib.label.str.contains('releve')

    gen_ch_lib['is_releve_chaudiere'] = is_releve_chaudiere.copy()

    is_releve = gen_ch_lib.groupby('td001_dpe_id').is_releve_chaudiere.max()

    del gen_ch_lib['is_releve_chaudiere']
    gen_ch_lib = gen_ch_lib.merge(is_releve, on='td001_dpe_id', how='left')

    add_releve_chaudiere = (~is_releve_chaudiere) & (gen_ch_lib.is_releve_chaudiere) & (
        gen_ch_lib.label.str.contains('pac'))
    add_releve_chaudiere = add_releve_chaudiere & (~gen_ch_lib.label.str.contains('air/air'))

    gen_ch_lib.loc[add_releve_chaudiere, 'label'] = gen_ch_lib.loc[
                                                        add_releve_chaudiere, 'label'] + ' en releve de chaudiere'

    # drop des labels  par défaut lorsqu'il existe mieux.

    ## drop chauffage electrique indetermine

    # on identifie les dpe pour lesquels on a des champs label elec determine (pac chaudiere ou effet joule) et le champs indetermine
    elec_ind = (gen_ch_lib.set_index('td001_dpe_id').label == 'chauffage electrique indetermine')
    elec_deter = gen_ch_lib.set_index('td001_dpe_id').label.str.contains('electrique|pac') & (~elec_ind)
    elec_deter = elec_deter.groupby('td001_dpe_id').max()
    elec_ind = elec_ind.groupby('td001_dpe_id').max()

    sel = elec_ind & elec_deter
    sel = sel[sel]
    sel = sel.to_frame('label').reset_index()
    sel['label'] = 'chauffage electrique indetermine'
    sel['to_drop'] = True

    gen_ch_lib = gen_ch_lib.merge(sel, on=['td001_dpe_id', 'label'], how='left')

    gen_ch_lib = gen_ch_lib.loc[gen_ch_lib.to_drop != True]
    del gen_ch_lib['to_drop']

    ## drop chauffage combustion indetermine

    # pour les autres energies les label contiennent l'energie de chauffage
    for ener in energie_combustion_mods:
        ener_ind = (gen_ch_lib.set_index('td001_dpe_id').label == f'chauffage {ener} indetermine')
        ener_deter = gen_ch_lib.set_index('td001_dpe_id').label.str.contains(f'{ener}') & (~ener_ind)
        ener_deter = ener_deter.groupby('td001_dpe_id').max()
        ener_ind = ener_ind.groupby('td001_dpe_id').max()

        sel = ener_ind & ener_deter
        sel = sel[sel]
        sel = sel.to_frame('label').reset_index()
        sel['label'] = f'chauffage {ener} indetermine'
        sel['to_drop'] = True
        gen_ch_lib = gen_ch_lib.merge(sel, on=['td001_dpe_id', 'label'], how='left')

        gen_ch_lib = gen_ch_lib.loc[gen_ch_lib.to_drop != True]
        del gen_ch_lib['to_drop']

    ## drop ecs electrique indetermine

    # on identifie les dpe pour lesquels on a des champs label elec determine (pac chaudiere ou effet joule) et le champs indetermine
    elec_ind = (gen_ecs_lib.set_index('td001_dpe_id').label == 'ecs electrique indetermine')
    elec_deter = gen_ecs_lib.set_index('td001_dpe_id').label.str.contains('electrique|thermodynamique') & (~elec_ind)
    elec_deter = elec_deter.groupby('td001_dpe_id').max()
    elec_ind = elec_ind.groupby('td001_dpe_id').max()

    sel = elec_ind & elec_deter
    sel = sel[sel]
    sel = sel.to_frame('label').reset_index()
    sel['label'] = 'ecs electrique indetermine'
    sel['to_drop'] = True

    gen_ecs_lib = gen_ecs_lib.merge(sel, on=['td001_dpe_id', 'label'], how='left')

    gen_ecs_lib = gen_ecs_lib.loc[gen_ecs_lib.to_drop != True]
    del gen_ecs_lib['to_drop']

    ## drop ecs combustion indetermine
    # pour les autres energies les label contiennent l'energie d'ecs
    for ener in energie_combustion_mods:
        ener_ind = (gen_ecs_lib.set_index('td001_dpe_id').label == f'ecs {ener} indetermine')
        ener_deter = gen_ecs_lib.set_index('td001_dpe_id').label.str.contains(f'{ener}') & (~ener_ind)
        ener_deter = ener_deter.groupby('td001_dpe_id').max()
        ener_ind = ener_ind.groupby('td001_dpe_id').max()

        sel = ener_ind & ener_deter
        sel = sel[sel]
        sel = sel.to_frame('label').reset_index()
        sel['label'] = f'ecs {ener} indetermine'
        sel['to_drop'] = True
        gen_ecs_lib = gen_ecs_lib.merge(sel, on=['td001_dpe_id', 'label'], how='left')

        gen_ecs_lib = gen_ecs_lib.loc[gen_ecs_lib.to_drop != True]
        del gen_ecs_lib['to_drop']

    # ===========================================concatenation des libéllés==========================================

    ## libéllé générateur chauffage

    gen_ch_lib['label'] = pd.Categorical(gen_ch_lib.label,
                                         categories=list(gen_ch_search_dict_flat.keys()) + ['indetermine'],
                                         ordered=True)

    gen_ch_lib = gen_ch_lib.sort_values(by=['td001_dpe_id', 'category', 'label', 'source'])

    gen_ch_lib = gen_ch_lib.drop_duplicates(subset=['td001_dpe_id', 'category'])

    gen_ch_lib = gen_ch_lib.groupby('td001_dpe_id').label.apply(lambda x: ' + '.join(np.unique(x)))

    gen_ch_lib = gen_ch_lib.to_frame('gen_ch_lib_final')

    td001_sys = td001_sys.merge(gen_ch_lib.reset_index(), on='td001_dpe_id', how='left')

    td001_sys.gen_ch_lib_final = td001_sys.gen_ch_lib_final.fillna('indetermine').astype(str)

    ## ajout du chauffage solaire au libéllé générateur

    solaire_ch_txt = pd.concat([solaire_ch_ft[['label', 'td001_dpe_id']], solaire_ch_desc[['label', 'td001_dpe_id']]],
                               axis=0)

    solaire_ch_txt = solaire_ch_txt.sort_values('label').drop_duplicates(subset=['td001_dpe_id'])

    solaire_ch_txt = solaire_ch_txt.loc[solaire_ch_txt.label == 'solaire']

    is_ch_solaire_txt = solaire_ch_txt.groupby('td001_dpe_id').label.count().astype(float) > 0

    td011_p['tr003_solaire'] = td011_p['tr003_solaire'].astype(float)

    is_ch_solaire_data = td011_p.groupby('td001_dpe_id').tr003_solaire.max() > 0

    td001_sys = td001_sys.merge(is_ch_solaire_data.to_frame('is_ch_solaire_data'), on='td001_dpe_id', how='left')

    td001_sys = td001_sys.merge(is_ch_solaire_txt.to_frame('is_ch_solaire_txt'), on='td001_dpe_id', how='left')

    td001_sys['is_ch_solaire'] = td001_sys.is_ch_solaire_txt.fillna(0).astype(
        float) + td001_sys.is_ch_solaire_data.fillna(0).astype(float)
    td001_sys['is_ch_solaire'] = td001_sys['is_ch_solaire'] > 0

    is_ch_solaire = td001_sys.is_ch_solaire == True

    td001_sys.loc[is_ch_solaire, 'gen_ch_lib_final'] = 'chauffage solaire + ' + td001_sys.loc[
        is_ch_solaire, 'gen_ch_lib_final']

    ## concatenation du type d'installation de chauffage.

    type_installation_ch['label'] = pd.Categorical(type_installation_ch['label'],
                                                   categories=['collectif', 'individuel', 'indetermine'],
                                                   ordered=True)

    type_installation_ch = type_installation_ch.sort_values(by=['td001_dpe_id', 'is_indetermine',
                                                                'source', 'label'])
    type_installation_ch = type_installation_ch.drop_duplicates(subset=['td001_dpe_id'])

    type_installation_ch = type_installation_ch.set_index('td001_dpe_id').label.to_frame('type_installation_ch')

    td001_sys = td001_sys.merge(type_installation_ch.reset_index(), on='td001_dpe_id', how='left')

    td001_sys.type_installation_ch = td001_sys.type_installation_ch.fillna('indetermine')

    ## concatenation du type d'energie de chauffage

    energie_ch['label'] = pd.Categorical(energie_ch['label'],
                                         categories=energie_normalise_ordered, ordered=True)
    energie_ch = energie_ch.sort_values(by=['td001_dpe_id', 'label', 'source'])

    energie_ch = energie_ch.drop_duplicates(subset=['td001_dpe_id', 'label'])

    energie_ch = energie_ch.groupby('td001_dpe_id').label.apply(lambda x: ' + '.join(np.unique(x))).to_frame(
        'type_energie_ch')

    td001_sys = td001_sys.merge(energie_ch.reset_index(), on='td001_dpe_id', how='left')

    td001_sys.type_energie_ch = td001_sys.type_energie_ch.fillna('indetermine')

    ## concatenation des libéllés de générateurs d'ECS

    gen_ecs_lib['label'] = pd.Categorical(gen_ecs_lib.label,
                                          categories=list(gen_ecs_search_dict_flat.keys()) + ['indetermine'],
                                          ordered=True)

    gen_ecs_lib['priorite'] = gen_ecs_lib.category.replace(priorisation_ecs)

    #### traitement des generateurs d'ECS prioritaires

    gen_ecs_lib_princ = gen_ecs_lib.query('priorite=="principal"')

    gen_ecs_lib_princ = gen_ecs_lib_princ.sort_values(by=['td001_dpe_id', 'category', 'label', 'source'])

    gen_ecs_lib_princ = gen_ecs_lib_princ.drop_duplicates(subset=['td001_dpe_id', 'category'])

    gen_ecs_lib_princ = gen_ecs_lib_princ.groupby('td001_dpe_id').label.apply(lambda x: ' + '.join(np.unique(x)))

    gen_ecs_lib_princ = gen_ecs_lib_princ.to_frame('gen_ecs_lib_final')

    td001_sys = td001_sys.merge(gen_ecs_lib_princ.reset_index(), on='td001_dpe_id', how='left')

    #### traitement des generateurs d'ecs secondaires

    gen_ecs_lib_sec = gen_ecs_lib.query('priorite=="secondaire"')

    gen_ecs_lib_sec = gen_ecs_lib_sec.sort_values(by=['td001_dpe_id', 'category', 'label', 'source'])

    gen_ecs_lib_sec = gen_ecs_lib_sec.drop_duplicates(subset=['td001_dpe_id', 'category'])

    gen_ecs_lib_sec = gen_ecs_lib_sec.groupby('td001_dpe_id').label.apply(lambda x: ' + '.join(np.unique(x)))

    gen_ecs_lib_sec = gen_ecs_lib_sec.to_frame('gen_ecs_lib_final_sec')

    td001_sys = td001_sys.merge(gen_ecs_lib_sec.reset_index(), on='td001_dpe_id', how='left')

    null = td001_sys.gen_ecs_lib_final.isnull()

    td001_sys.loc[null, 'gen_ecs_lib_final'] = td001_sys.loc[null, 'gen_ecs_lib_final_sec']

    #### traitement des generateurs d'ecs par défaut

    gen_ecs_lib_defaut = gen_ecs_lib.query('priorite=="defaut"')

    gen_ecs_lib_defaut = gen_ecs_lib_defaut.sort_values(by=['td001_dpe_id', 'category', 'label', 'source'])

    gen_ecs_lib_defaut = gen_ecs_lib_defaut.drop_duplicates(subset=['td001_dpe_id', 'category'])

    gen_ecs_lib_defaut = gen_ecs_lib_defaut.groupby('td001_dpe_id').label.apply(lambda x: ' + '.join(np.unique(x)))

    gen_ecs_lib_defaut = gen_ecs_lib_defaut.to_frame('gen_ecs_lib_final_defaut')

    td001_sys = td001_sys.merge(gen_ecs_lib_defaut.reset_index(), on='td001_dpe_id', how='left')

    null = td001_sys.gen_ecs_lib_final.isnull()

    td001_sys.loc[null, 'gen_ecs_lib_final'] = td001_sys.loc[null, 'gen_ecs_lib_final_defaut']

    td001_sys.gen_ecs_lib_final = td001_sys.gen_ecs_lib_final.fillna('indetermine')

    ####  ecs solaire

    gen_ecs_lib_solaire = gen_ecs_lib.query('priorite=="solaire"').copy()

    gen_ecs_lib_solaire.label = pd.Categorical(gen_ecs_lib_solaire.label, ['abscence ecs solaire', 'ecs solaire'],
                                               ordered=True)

    gen_ecs_lib_solaire = gen_ecs_lib_solaire.sort_values('label', ascending=True).drop_duplicates(
        subset='td001_dpe_id')

    gen_ecs_lib_solaire = gen_ecs_lib_solaire.loc[gen_ecs_lib_solaire.label == 'ecs solaire']

    is_ecs_solaire_txt = (gen_ecs_lib_solaire.groupby('td001_dpe_id').label.count().astype(float) > 0)

    is_ecs_solaire_data = td014_p.groupby('td001_dpe_id').is_ecs_solaire.max().astype(float) > 0

    td001_sys = td001_sys.merge(is_ecs_solaire_txt.to_frame('is_ecs_solaire_txt'), how='left', on='td001_dpe_id')
    td001_sys = td001_sys.merge(is_ecs_solaire_data.to_frame('is_ecs_solaire_data'), how='left', on='td001_dpe_id')

    td001_sys['is_ecs_solaire'] = td001_sys.is_ecs_solaire_txt.fillna(0).astype(
        float) + td001_sys.is_ecs_solaire_data.fillna(0).astype(float)
    td001_sys['is_ecs_solaire'] = td001_sys['is_ecs_solaire'] > 0

    is_ecs_solaire = td001_sys.is_ecs_solaire == True

    td001_sys.loc[is_ecs_solaire, 'gen_ecs_lib_final'] = 'ecs solaire + ' + td001_sys.loc[
        is_ecs_solaire, 'gen_ecs_lib_final']

    #### redressement combinaison mixte indetermine et energie de combustion

    is_production_mixte = td001_sys.gen_ecs_lib_final == "production mixte indetermine"
    for ener in energie_chaudiere_mods:
        is_ener = td001_sys.gen_ecs_lib_final_defaut.str.contains(f'ecs {ener} indetermine')
        td001_sys.loc[is_production_mixte & is_ener, 'gen_ecs_lib_final'] = f'chaudiere {ener} indetermine'

    is_production_mixte = td001_sys.gen_ecs_lib_final == "production mixte indetermine"

    is_ener = td001_sys.gen_ecs_lib_final_defaut.str.contains('ecs bois indetermine')
    td001_sys.loc[is_production_mixte & is_ener, 'gen_ecs_lib_final'] = f'chaudiere bois'

    ### type d'installation d'ECS

    type_installation_ecs['label'] = pd.Categorical(type_installation_ecs['label'],
                                                    categories=['collectif', 'individuel', 'indetermine'], ordered=True)
    type_installation_ecs = type_installation_ecs.sort_values(by=['td001_dpe_id', 'is_indetermine',
                                                                  'source', 'label'])
    type_installation_ecs = type_installation_ecs.drop_duplicates(subset=['td001_dpe_id'])

    type_installation_ecs = type_installation_ecs.set_index('td001_dpe_id').label.to_frame('type_installation_ecs')

    td001_sys = td001_sys.merge(type_installation_ecs.reset_index(), on='td001_dpe_id', how='left')

    td001_sys.type_installation_ecs = td001_sys.type_installation_ecs.fillna('indetermine')

    ### type d'energie d'ECS

    energie_ecs['label'] = pd.Categorical(energie_ecs['label'],
                                          categories=energie_normalise_ordered, ordered=True)

    energie_ecs = energie_ecs.sort_values(by=['td001_dpe_id', 'label', 'source'])

    energie_ecs = energie_ecs.drop_duplicates(subset=['td001_dpe_id', 'label'])

    energie_ecs = energie_ecs.groupby('td001_dpe_id').label.apply(lambda x: ' + '.join(np.unique(x))).to_frame(
        'type_energie_ecs')

    td001_sys = td001_sys.merge(energie_ecs.reset_index(), on='td001_dpe_id', how='left')
    td001_sys.type_energie_ecs = td001_sys.type_energie_ecs.fillna('indetermine')

    return td001_sys


def concat_and_sort_gen_ch_lib(gen_ch_lib_desc, gen_ch_lib_ft, td011_p, td012_p):
    """

    assemblage des libéllés de chauffage extraits des fiches techniques/descriptifs et extrait du modèle de donnée affilié 3CL.
    chaque libéllé a une catégorie parente affectée.

    Règle de simplification n°1 :On ne peut avoir deux systèmes de la même catégorie dans la description des générateurs de chauffage
    i.e chaudiere gaz condensation et chaudiere fioul standard sont dans la même catégorie "chaudiere". Un seul libéllé sera retenu.

    Règle n°2  : les libéllés sont ordonnées selon 3 critères :

    * determination :  si un libéllé dans une catégorie est mieux déterminé qu'un autre alors il est classé plus haut (chaudiere gaz standard > chaudiere gaz indetermine)

    * probabilité d'être le système principal :  chaudiere gaz standard > chaudiere fioul standard. Si on a fioul et gaz dans un logement la probabilité est bien plus grande d'avoir chaudière gaz + appoint fioul que l'inverse

    * identification certaine : si certains libéllés sont identifiables avec

    Règle n°3  : les données ne sont pour l'instant pas priorisées si elles viennent du modèle de données 3CL vs description texte.

    Returns
    -------

    """

    # libéllés des generateurs chauffage issues des données textes
    gen_ch_lib_from_txt = pd.concat([gen_ch_lib_desc[['label', 'category', 'td001_dpe_id']],
                                     gen_ch_lib_ft[['label', 'category', 'td001_dpe_id']]], axis=0)

    gen_ch_lib_from_txt['label'] = pd.Categorical(gen_ch_lib_from_txt.label,
                                                  categories=list(gen_ch_search_dict_flat.keys()) + ['indetermine'],
                                                  ordered=True)

    gen_ch_lib_from_txt = gen_ch_lib_from_txt.sort_values(by=['td001_dpe_id', 'category', 'label'])

    gen_ch_lib_from_txt = gen_ch_lib_from_txt.drop_duplicates(subset=['category', 'td001_dpe_id'], keep='first')
    # suppression de la dénomination exact qui ne correpond qu'a une méthode de recherche plus rigide
    gen_ch_lib_from_txt.label = gen_ch_lib_from_txt.label.str.replace(' exact', '')

    gen_ch_lib_from_txt['source'] = 'txt'

    # libéllés des générateurs chauffage issus de la table td012
    gen_ch_lib_from_data = td012_p[['td001_dpe_id', 'gen_ch_lib_infer']].copy()

    gen_ch_lib_from_data.columns = ['td001_dpe_id', 'label']

    gen_ch_lib_from_data['category'] = gen_ch_lib_from_data.label.replace(reverse_cat_gen_ch)

    gen_ch_lib_from_data['label'] = pd.Categorical(gen_ch_lib_from_data.label,
                                                   categories=list(gen_ch_search_dict_flat.keys()) + ['indetermine'],
                                                   ordered=True)
    gen_ch_lib_from_data['source'] = 'data'

    tr003_desc = td011_p.loc[td011_p.tr003_description.isin(tr003_desc_to_gen.keys())]

    tr003_desc = tr003_desc[['td001_dpe_id', 'tr003_description']]
    tr003_desc.columns = ['td001_dpe_id', 'label']
    tr003_desc.label = tr003_desc.label.replace(tr003_desc_to_gen)
    tr003_desc['category'] = tr003_desc.label.replace(reverse_cat_gen_ch)

    tr003_desc['label'] = pd.Categorical(tr003_desc.label,
                                         categories=list(gen_ch_search_dict_flat.keys()) + ['indetermine'],
                                         ordered=True)
    tr003_desc['source'] = 'data'

    # concat.
    gen_ch_lib = pd.concat([gen_ch_lib_from_data, tr003_desc, gen_ch_lib_from_txt], axis=0)

    gen_ch_lib['source'] = pd.Categorical(gen_ch_lib.source, categories=['data', 'txt'], ordered=True)
    gen_ch_lib['category'] = pd.Categorical(gen_ch_lib.category, categories=list(gen_ch_search_dict.keys()),
                                            ordered=True)

    gen_ch_lib = gen_ch_lib.reset_index(drop=True)

    return gen_ch_lib


def concat_and_sort_type_installation_ch_lib(type_installation_ch_desc, type_installation_ch_ft, td011_p):
    """

    assemblage des libéllés de type installation de chauffage extraits des fiches techniques/descriptifs et extrait du modèle de donnée affilié 3CL.
    on fait un tri collectif>individuel (si on trouve la mention de termes collectifs on les priorise par rapports aux termes individuels

    -------

    """

    # libéllés des generateurs chauffage issues des données textes
    type_installation_ch_from_txt = pd.concat([type_installation_ch_desc[['label', 'td001_dpe_id']],
                                               type_installation_ch_ft[['label', 'td001_dpe_id']]], axis=0)

    type_installation_ch_from_txt['label'] = pd.Categorical(type_installation_ch_from_txt['label'],
                                                            categories=['collectif', 'individuel', 'indetermine'],
                                                            ordered=True)

    type_installation_ch_from_txt = type_installation_ch_from_txt.sort_values(by=['td001_dpe_id', 'label'])

    type_installation_ch_from_txt = type_installation_ch_from_txt.drop_duplicates(subset=['td001_dpe_id'], keep='first')

    type_installation_ch_from_txt['source'] = 'txt'

    type_installation_ch_from_data = td011_p[['td001_dpe_id', 'type_installation_ch']].copy()
    type_installation_ch_from_data.columns = ['td001_dpe_id', 'label']
    type_installation_ch_from_data['label'] = type_installation_ch_from_data['label'].fillna('indetermine')
    type_installation_ch_from_data['label'] = pd.Categorical(type_installation_ch_from_data['label'],
                                                             categories=['collectif', 'individuel', 'indetermine'],
                                                             ordered=True)
    type_installation_ch_from_data['source'] = 'data'
    type_installation_ch = pd.concat([type_installation_ch_from_data, type_installation_ch_from_txt], axis=0)

    type_installation_ch['is_indetermine'] = type_installation_ch.label == 'indetermine'

    return type_installation_ch


def concat_and_sort_energie_ch_lib(energie_ch_desc, energie_ch_ft, td012_p):
    """

    assemblage des libéllés de l'energie extraits des fiches techniques/descriptifs et extrait du modèle de donnée affilié 3CL.

    -------

    """

    # libéllés des generateurs chauffage issues des données textes
    energie_ch_from_txt = pd.concat([energie_ch_desc[['label', 'td001_dpe_id']],
                                     energie_ch_ft[['label', 'td001_dpe_id']]], axis=0)

    energie_ch_from_txt['label'] = pd.Categorical(energie_ch_from_txt['label'],
                                                  categories=energie_normalise_ordered, ordered=True)

    energie_ch_from_txt = energie_ch_from_txt.sort_values(by=['td001_dpe_id', 'label'])

    energie_ch_from_txt['source'] = 'txt'

    energie_ch_from_data = td012_p[['td001_dpe_id', 'type_energie_ch']].copy()
    energie_ch_from_data.columns = ['td001_dpe_id', 'label']
    energie_ch_from_data['label'] = energie_ch_from_data['label'].fillna('indetermine')
    energie_ch_from_data['label'] = pd.Categorical(energie_ch_from_data['label'],
                                                   categories=energie_normalise_ordered, ordered=True)
    energie_ch_from_data['source'] = 'data'
    energie_ch = pd.concat([energie_ch_from_data, energie_ch_from_txt], axis=0)

    energie_ch['is_indetermine'] = energie_ch.label == 'indetermine'

    return energie_ch


def concat_and_sort_gen_ecs_lib(gen_ecs_lib_desc, gen_ecs_lib_ft, td014_p):
    """

    assemblage des libéllés de chauffage extraits des fiches techniques/descriptifs et extrait du modèle de donnée affilié 3CL.
    chaque libéllé a une catégorie parente affectée.

    Règle de simplification n°1 :On ne peut avoir deux systèmes de la même catégorie dans la description des générateurs de chauffage
    i.e chaudiere gaz condensation et chaudiere fioul standard sont dans la même catégorie "chaudiere". Un seul libéllé sera retenu.

    Règle n°2  : les libéllés sont ordonnées selon 3 critères :

    * determination :  si un libéllé dans une catégorie est mieux déterminé qu'un autre alors il est classé plus haut (chaudiere gaz standard > chaudiere gaz indetermine)

    * probabilité d'être le système principal :  chaudiere gaz standard > chaudiere fioul standard. Si on a fioul et gaz dans un logement la probabilité est bien plus grande d'avoir chaudière gaz + appoint fioul que l'inverse

    * identification certaine : si certains libéllés sont identifiables avec

    Règle n°3  : les données ne sont pour l'instant pas priorisées si elles viennent du modèle de données 3CL vs description texte.

    Returns
    -------

    """

    # libéllés des generateurs ecs issues des données textes
    gen_ecs_lib_from_txt = pd.concat([gen_ecs_lib_desc[['label', 'category', 'td001_dpe_id']],
                                      gen_ecs_lib_ft[['label', 'category', 'td001_dpe_id']]], axis=0)

    gen_ecs_lib_from_txt['label'] = pd.Categorical(gen_ecs_lib_from_txt.label,
                                                   categories=list(gen_ecs_search_dict_flat.keys()) + ['indetermine'],
                                                   ordered=True)

    gen_ecs_lib_from_txt = gen_ecs_lib_from_txt.sort_values(by=['td001_dpe_id', 'category', 'label'])

    gen_ecs_lib_from_txt = gen_ecs_lib_from_txt.drop_duplicates(subset=['category', 'td001_dpe_id'], keep='first')

    # suppression de la dénomination exact qui ne correpond qu'a une méthode de recherche plus rigide
    gen_ecs_lib_from_txt.label = gen_ecs_lib_from_txt.label.str.replace(' exact', '')

    gen_ecs_lib_from_txt['source'] = 'txt'

    # libéllés des générateurs ecs issus de la table td014
    gen_ecs_lib_from_data = td014_p[['td001_dpe_id', 'gen_ecs_lib_infer']].copy()

    gen_ecs_lib_from_data.columns = ['td001_dpe_id', 'label']
    gen_ecs_lib_from_data['label'] = gen_ecs_lib_from_data['label'].fillna('indetermine')
    gen_ecs_lib_from_data['category'] = gen_ecs_lib_from_data.label.replace(reverse_cat_gen_ecs)

    gen_ecs_lib_from_data['label'] = pd.Categorical(gen_ecs_lib_from_data.label,
                                                    categories=list(gen_ecs_search_dict_flat.keys()) + ['indetermine'],
                                                    ordered=True)
    gen_ecs_lib_from_data['label'] = gen_ecs_lib_from_data['label'].fillna('indetermine')

    gen_ecs_lib_from_data['source'] = 'data'

    # concat.
    gen_ecs_lib = pd.concat([gen_ecs_lib_from_data, gen_ecs_lib_from_txt], axis=0)

    gen_ecs_lib['source'] = pd.Categorical(gen_ecs_lib.source, categories=['data', 'txt'], ordered=True)

    return gen_ecs_lib


def concat_and_sort_type_installation_ecs_lib(type_installation_ecs_desc, type_installation_ecs_ft, td014_p):
    """

    assemblage des libéllés de type installation de chauffage extraits des fiches techniques/descriptifs et extrait du modèle de donnée affilié 3CL.
    on fait un tri collectif>individuel (si on trouve la mention de termes collectifs on les priorise par rapports aux termes individuels

    -------

    """

    # libéllés des installations ecs issues des données textes
    type_installation_ecs_from_txt = pd.concat([type_installation_ecs_desc[['label', 'td001_dpe_id']],
                                                type_installation_ecs_ft[['label', 'td001_dpe_id']]], axis=0)

    type_installation_ecs_from_txt['label'] = pd.Categorical(type_installation_ecs_from_txt['label'],
                                                             categories=['collectif', 'individuel', 'indetermine'],
                                                             ordered=True)

    type_installation_ecs_from_txt = type_installation_ecs_from_txt.sort_values(by=['td001_dpe_id', 'label'])

    type_installation_ecs_from_txt = type_installation_ecs_from_txt.drop_duplicates(subset=['td001_dpe_id'],
                                                                                    keep='first')

    type_installation_ecs_from_txt['source'] = 'txt'

    type_installation_ecs_from_data = td014_p[['td001_dpe_id', 'type_installation_ecs']].copy()
    type_installation_ecs_from_data.columns = ['td001_dpe_id', 'label']
    type_installation_ecs_from_data['label'] = type_installation_ecs_from_data['label'].fillna('indetermine')
    type_installation_ecs_from_data['label'] = pd.Categorical(type_installation_ecs_from_data['label'],
                                                              categories=['collectif', 'individuel', 'indetermine'],
                                                              ordered=True)
    type_installation_ecs_from_data['source'] = 'data'
    type_installation_ecs = pd.concat([type_installation_ecs_from_data, type_installation_ecs_from_txt], axis=0)

    type_installation_ecs['is_indetermine'] = type_installation_ecs.label == 'indetermine'

    return type_installation_ecs


def concat_and_sort_energie_ecs_lib(energie_ecs_desc, energie_ecs_ft, td014_p):
    """

    assemblage des libéllés de l'energie extraits des fiches techniques/descriptifs et extrait du modèle de donnée affilié 3CL.

    -------

    """

    # libéllés des energies d'ecs issues des données textes
    energie_ecs_from_txt = pd.concat([energie_ecs_desc[['label', 'td001_dpe_id']],
                                      energie_ecs_ft[['label', 'td001_dpe_id']]], axis=0)

    energie_ecs_from_txt['label'] = pd.Categorical(energie_ecs_from_txt['label'],
                                                   categories=energie_normalise_ordered, ordered=True)

    energie_ecs_from_txt = energie_ecs_from_txt.sort_values(by=['td001_dpe_id', 'label'])

    energie_ecs_from_txt['source'] = 'txt'

    energie_ecs_from_data = td014_p[['td001_dpe_id', 'type_energie_ecs']].copy()
    energie_ecs_from_data.columns = ['td001_dpe_id', 'label']
    energie_ecs_from_data['label'] = energie_ecs_from_data['label'].fillna('indetermine')
    energie_ecs_from_data['label'] = pd.Categorical(energie_ecs_from_data['label'],
                                                    categories=energie_normalise_ordered, ordered=True)
    energie_ecs_from_data['source'] = 'data'
    energie_ecs = pd.concat([energie_ecs_from_data, energie_ecs_from_txt], axis=0)

    energie_ecs['is_indetermine'] = energie_ecs.label == 'indetermine'

    return energie_ecs


def redressement_td001_sys(td001_sys):
    # remove trailing indetermine
    td001_sys['gen_ch_lib_final'] = td001_sys.gen_ch_lib_final.str.replace(' \+ indetermine', '',regex=False)
    td001_sys['gen_ecs_lib_final'] = td001_sys.gen_ecs_lib_final.str.replace(' \+ indetermine', '',regex=False)

    td001_sys['gen_ch_lib_non_retraite'] = td001_sys.gen_ch_lib_final.copy()
    td001_sys['gen_ecs_lib_non_retraite'] = td001_sys.gen_ecs_lib_final.copy()

    ind = td001_sys.gen_ch_lib_final.fillna('indetermine').str.contains('energie indetermine')

    # ============= retraitement des energies non affectées pour les chaudières avec les energies factures ============

    # CHAUFFAGE

    ind = td001_sys.gen_ch_lib_final.fillna('indetermine').str.contains('energie indetermine')

    for energie in energie_chaudiere_mods:
        # ON AFFECTE LES ENERGIES MANQUANTES AVEC LES ENERGIES DE CHAUFFAGE ISSUES DIFFERENTES SOURCES

        ener = td001_sys.td002_type_energie_ch.str.contains(energie)
        td001_sys.loc[ener & ind, 'gen_ch_lib_final'] = td001_sys.loc[ener & ind, 'gen_ch_lib_final'].str.replace(
            'energie indetermine', energie)
        ind = td001_sys.gen_ch_lib_final.fillna('indetermine').str.contains('energie indetermine')

        ener = td001_sys.td016_type_energie_ch.str.contains(energie)
        td001_sys.loc[ener & ind, 'gen_ch_lib_final'] = td001_sys.loc[ener & ind, 'gen_ch_lib_final'].str.replace(
            'energie indetermine', energie)
        ind = td001_sys.gen_ch_lib_final.fillna('indetermine').str.contains('energie indetermine')

        ener = td001_sys.type_energie_ch.str.contains(energie)
        td001_sys.loc[ener & ind, 'gen_ch_lib_final'] = td001_sys.loc[ener & ind, 'gen_ch_lib_final'].str.replace(
            'energie indetermine', energie)
        ind = td001_sys.gen_ch_lib_final.fillna('indetermine').str.contains('energie indetermine')

        # SINON ON PREND LES ENERGIES D'ECS ISSUES DES DIFFERENTES SOURCES

        ener = td001_sys.type_energie_ecs.str.contains(energie)
        td001_sys.loc[ener & ind, 'gen_ch_lib_final'] = td001_sys.loc[ener & ind, 'gen_ch_lib_final'].str.replace(
            'energie indetermine', energie)
        ind = td001_sys.gen_ch_lib_final.fillna('indetermine').str.contains('energie indetermine')

        ener = td001_sys.td002_type_energie_ecs.str.contains(energie)
        td001_sys.loc[ener & ind, 'gen_ch_lib_final'] = td001_sys.loc[ener & ind, 'gen_ch_lib_final'].str.replace(
            'energie indetermine', energie)
        ind = td001_sys.gen_ch_lib_final.fillna('indetermine').str.contains('energie indetermine')

        ener = td001_sys.td016_type_energie_ecs.str.contains(energie)
        td001_sys.loc[ener & ind, 'gen_ch_lib_final'] = td001_sys.loc[ener & ind, 'gen_ch_lib_final'].str.replace(
            'energie indetermine', energie)
        ind = td001_sys.gen_ch_lib_final.fillna('indetermine').str.contains('energie indetermine')

    # Gestion des poeles indetermine
    poele_ind = td001_sys.gen_ch_lib_final.str.contains('poele ou insert indetermine')

    bois = td001_sys.td002_type_energie_ch.str.contains('bois')
    td001_sys.loc[bois & poele_ind, 'gen_ch_lib_final'] = td001_sys.loc[
        bois & poele_ind, 'gen_ch_lib_final'].str.replace('poele ou insert indetermine',
                                                          'poele ou insert bois')
    bois = td001_sys.td016_type_energie_ch.str.contains('bois')
    td001_sys.loc[bois & poele_ind, 'gen_ch_lib_final'] = td001_sys.loc[
        bois & poele_ind, 'gen_ch_lib_final'].str.replace('poele ou insert indetermine',
                                                          'poele ou insert bois')

    fioul = td001_sys.td002_type_energie_ch.str.contains('fioul')
    td001_sys.loc[fioul & poele_ind, 'gen_ch_lib_final'] = td001_sys.loc[
        fioul & poele_ind, 'gen_ch_lib_final'].str.replace('poele ou insert indetermine',
                                                           'poele ou insert fioul')
    fioul = td001_sys.td016_type_energie_ch.str.contains('fioul')
    td001_sys.loc[fioul & poele_ind, 'gen_ch_lib_final'] = td001_sys.loc[
        fioul & poele_ind, 'gen_ch_lib_final'].str.replace('poele ou insert indetermine',
                                                           'poele ou insert fioul')

    gpl = td001_sys.td002_type_energie_ch.str.contains('gpl/butane/propane')
    td001_sys.loc[gpl & poele_ind, 'gen_ch_lib_final'] = td001_sys.loc[
        gpl & poele_ind, 'gen_ch_lib_final'].str.replace('poele ou insert indetermine',
                                                         'poele ou insert gpl/butane/propane')
    gpl = td001_sys.td016_type_energie_ch.str.contains('gpl/butane/propane')
    td001_sys.loc[gpl & poele_ind, 'gen_ch_lib_final'] = td001_sys.loc[
        gpl & poele_ind, 'gen_ch_lib_final'].str.replace('poele ou insert indetermine',
                                                         'poele ou insert gpl/butane/propane')

    # ECS

    ind = td001_sys.gen_ecs_lib_final.fillna('indetermine').str.contains('energie indetermine')

    for energie in energie_chaudiere_mods:
        # ON AFFECTE LES ENERGIES MANQUANTES AVEC LES ENERGIES D'ECS ISSUES DIFFERENTES SOURCES

        ener = td001_sys.td002_type_energie_ecs.str.contains(energie)
        td001_sys.loc[ener & ind, 'gen_ecs_lib_final'] = td001_sys.loc[ener & ind, 'gen_ecs_lib_final'].str.replace(
            'energie indetermine', energie)
        ind = td001_sys.gen_ecs_lib_final.fillna('indetermine').str.contains('energie indetermine')
        ener = td001_sys.td016_type_energie_ecs.str.contains(energie)
        td001_sys.loc[ener & ind, 'gen_ecs_lib_final'] = td001_sys.loc[ener & ind, 'gen_ecs_lib_final'].str.replace(
            'energie indetermine', energie)
        ind = td001_sys.gen_ecs_lib_final.fillna('indetermine').str.contains('energie indetermine')

        ener = td001_sys.type_energie_ecs.str.contains(energie)
        td001_sys.loc[ener & ind, 'gen_ecs_lib_final'] = td001_sys.loc[ener & ind, 'gen_ecs_lib_final'].str.replace(
            'energie indetermine', energie)
        ind = td001_sys.gen_ecs_lib_final.fillna('indetermine').str.contains('energie indetermine')

        # SINON ON PREND LES ENERGIES DE CHAUFFAGE ISSUES DES DIFFERENTES SOURCES

        ener = td001_sys.type_energie_ch.str.contains(energie)
        td001_sys.loc[ener & ind, 'gen_ecs_lib_final'] = td001_sys.loc[ener & ind, 'gen_ecs_lib_final'].str.replace(
            'energie indetermine', energie)
        ind = td001_sys.gen_ecs_lib_final.fillna('indetermine').str.contains('energie indetermine')

        ener = td001_sys.td002_type_energie_ch.str.contains(energie)
        td001_sys.loc[ener & ind, 'gen_ecs_lib_final'] = td001_sys.loc[ener & ind, 'gen_ecs_lib_final'].str.replace(
            'energie indetermine', energie)
        ind = td001_sys.gen_ecs_lib_final.fillna('indetermine').str.contains('energie indetermine')

        ener = td001_sys.td016_type_energie_ch.str.contains(energie)
        td001_sys.loc[ener & ind, 'gen_ecs_lib_final'] = td001_sys.loc[ener & ind, 'gen_ecs_lib_final'].str.replace(
            'energie indetermine', energie)
        ind = td001_sys.gen_ecs_lib_final.fillna('indetermine').str.contains('energie indetermine')

    # ==================================== Cohérence entre les systèmes d'ECS et de chauffage =====================

    td001_sys[['gen_ch_lib_final', 'gen_ecs_lib_final']] = td001_sys[
        ['gen_ch_lib_final', 'gen_ecs_lib_final']].fillna('indetermine')

    is_ind_ecs = td001_sys.gen_ecs_lib_final.str.contains('production mixte indetermine')
    is_ind_ecs = is_ind_ecs | (td001_sys.gen_ecs_lib_final == "indetermine")
    is_ind_ch = td001_sys.gen_ch_lib_final == "indetermine"

    # ================================== CHAUDIERES  ==============================================================

    td001_sys['chaudiere_ecs'] = td001_sys['gen_ecs_lib_final'].apply(
        lambda x: ' + '.join([el for el in x.split(' + ') if 'chaudiere' in el]))
    td001_sys['chaudiere_ch'] = td001_sys['gen_ch_lib_final'].apply(
        lambda x: ' + '.join([el for el in x.split(' + ') if ('chaudiere' in el) and not ('releve' in el)]))

    is_chaudiere_ener_ind_ch = td001_sys.gen_ch_lib_final.str.contains('chaudiere energie indetermine')
    is_chaudiere_ener_ind_ecs = td001_sys.gen_ecs_lib_final.str.contains('chaudiere energie indetermine')
    is_chaudiere_ch = td001_sys.gen_ch_lib_final.str.contains('chaudiere')
    is_chaudiere_ch_only = is_chaudiere_ch & (~td001_sys.gen_ch_lib_final.str.contains('pac'))
    is_chaudiere_ecs = td001_sys.gen_ecs_lib_final.str.contains('chaudiere')

    #### affectation du système mixte à l'ECS lorsque celle ci est indetermine

    sub_by_ch = is_ind_ecs & is_chaudiere_ch_only

    td001_sys.loc[sub_by_ch, 'gen_ecs_lib_final'] = td001_sys.loc[sub_by_ch, 'chaudiere_ch']

    #### correction et convergence des energies de chaudieres entre ECS et chauffage

    for energie in list(reversed(energie_chaudiere_mods)):
        is_ener_ch = (td001_sys.gen_ch_lib_final.str.contains(f'chaudiere {energie}'))
        is_ener_ecs = (td001_sys.gen_ecs_lib_final.str.contains(f'chaudiere {energie}'))
        sub_ch_ecs = is_ener_ch & (~is_ener_ecs) & is_chaudiere_ch_only & is_chaudiere_ecs
        td001_sys.loc[sub_ch_ecs, 'gen_ecs_lib_final'] = td001_sys.loc[sub_ch_ecs, 'gen_ecs_lib_final'].str.replace(
            'energie indetermine', energie)
        sub_ecs_ch = is_ener_ecs & (~is_ener_ch) & is_chaudiere_ch_only & is_chaudiere_ecs
        td001_sys.loc[sub_ecs_ch, 'gen_ch_lib_final'] = td001_sys.loc[sub_ecs_ch, 'gen_ch_lib_final'].str.replace(
            'energie indetermine', energie)

    #### correction et convergence des types de chaudieres entre ECS et chauffage

    for energie in reversed(energie_chaudiere_mods):
        is_ener_ch = (td001_sys.gen_ch_lib_final.str.contains(energie))
        is_ener_ecs = (td001_sys.gen_ecs_lib_final.str.contains(energie))
        both_ener = is_ener_ch & is_ener_ecs & is_chaudiere_ch & is_chaudiere_ecs
        for type_chaudiere in reversed(type_chaudiere_mods):
            is_type_ch = (td001_sys.gen_ch_lib_final.str.contains(type_chaudiere))
            is_type_ecs = (td001_sys.gen_ecs_lib_final.str.contains(type_chaudiere))
            chaudiere_ind = f'chaudiere {energie} indetermine'
            chaudiere_type = f'chaudiere {energie} {type_chaudiere}'
            is_ind_ch = (td001_sys.gen_ch_lib_final.str.contains(chaudiere_ind))
            is_ind_ecs = (td001_sys.gen_ecs_lib_final.str.contains(chaudiere_ind))
            sub_ch_ecs = is_type_ch & both_ener & is_ind_ecs
            td001_sys.loc[sub_ch_ecs, 'gen_ecs_lib_final'] = td001_sys.loc[
                sub_ch_ecs, 'gen_ecs_lib_final'].str.replace(chaudiere_ind, chaudiere_type)
            sub_ecs_ch = is_type_ecs & both_ener & is_ind_ch
            td001_sys.loc[sub_ecs_ch, 'gen_ch_lib_final'] = td001_sys.loc[
                sub_ecs_ch, 'gen_ch_lib_final'].str.replace(chaudiere_ind, chaudiere_type)

    #### correction cas ECS affecté à un chauffe eau indépendant lorsque chaudiere chauffage

    # refresh
    is_chaudiere_ch_only = td001_sys.gen_ch_lib_final.str.contains('chaudiere') & (
        ~td001_sys.gen_ch_lib_final.str.contains('pac'))
    is_ind = td001_sys.gen_ecs_lib_final.str.contains('chauffe-eau independant indetermine')
    is_ind = is_ind | (td001_sys.gen_ecs_lib_final == 'indetermine')

    td001_sys.loc[is_chaudiere_ch_only & is_ind, 'gen_ecs_lib_final'] = td001_sys.loc[
        is_chaudiere_ch_only & is_ind, 'chaudiere_ch']

    #### correction ecs partie chaudiere -> affecté au chauffage par défaut

    is_chaudiere_ecs_and_ch = is_chaudiere_ecs & is_chaudiere_ch_only

    td001_sys.loc[is_chaudiere_ecs_and_ch, 'not_chaudiere_ecs'] = td001_sys.loc[
        is_chaudiere_ecs_and_ch, 'gen_ecs_lib_final'].apply(
        lambda x: ' + '.join([el for el in x.split(' + ') if 'chaudiere' not in el]))
    td001_sys.loc[is_chaudiere_ecs_and_ch, 'not_chaudiere_ch'] = td001_sys.loc[
        is_chaudiere_ecs_and_ch, 'gen_ch_lib_final'].apply(
        lambda x: ' + '.join([el for el in x.split(' + ') if 'chaudiere' not in el]))

    sel = (td001_sys['chaudiere_ch'] != td001_sys.chaudiere_ecs) & is_chaudiere_ecs_and_ch

    td001_sys.loc[sel, "chaudiere_ecs"] = td001_sys.loc[sel, 'chaudiere_ch']

    td001_sys.loc[sel, 'gen_ecs_lib_final'] = td001_sys.loc[sel, 'chaudiere_ecs']
    sel = sel & (td001_sys.not_chaudiere_ecs != '')
    td001_sys.loc[sel, 'gen_ecs_lib_final'] = td001_sys.loc[sel, 'gen_ecs_lib_final'] + ' + ' + td001_sys.loc[
        sel, 'not_chaudiere_ecs']

    #### correction cas ecs si gen_ch_lib elec ou poele

    # si le generateur de chauffage est décentralisé et avec une energie non combustible.
    is_ind = td001_sys.gen_ecs_lib_final.str.contains('chauffe-eau independant indetermine')
    is_ind = is_ind | (td001_sys.gen_ecs_lib_final == 'indetermine')
    is_ind = is_ind | (td001_sys.gen_ecs_lib_final.str.contains('production mixte indetermine'))

    is_ch_decentr = ~td001_sys.gen_ch_lib_final.str.contains('|'.join(energie_chaudiere_mods))
    is_ch_decentr = is_ch_decentr & td001_sys.gen_ch_lib_final.str.contains('elec|pac|joule|jonction|poele')

    td001_sys.loc[is_ind & is_ch_decentr, 'gen_ecs_lib_final'] = 'ecs electrique indetermine'

    # ================================ RESEAUX DE CHALEUR ========================================================

    # si le chauffage ou l'ECS sont indéterminés et que l'autre est un réseau de chaleur alors on substitue par réseau de chaleur
    is_ind_ecs = td001_sys.gen_ecs_lib_final.str.contains('production mixte indetermine')
    is_ind_ecs = is_ind_ecs | (td001_sys.gen_ecs_lib_final == "indetermine")
    is_ind_ch = td001_sys.gen_ch_lib_final == "indetermine"

    is_reseau_ch = td001_sys.gen_ch_lib_final.str.contains('reseau de chaleur')
    is_reseau_ecs = td001_sys.gen_ecs_lib_final.str.contains('reseau de chaleur')

    td001_sys.loc[is_ind_ch & is_reseau_ecs, 'gen_ch_lib_final'] = 'reseau de chaleur'
    td001_sys.loc[is_ind_ecs & is_reseau_ch, 'gen_ecs_lib_final'] = 'reseau de chaleur'

    # ===================================== THERMODYNAMIQUE =======================================================

    # si l'un est thermodynamique et l'autre indeterminé on considère l'autre comme thermodynamique.
    is_ind_ecs = td001_sys.gen_ecs_lib_final.str.contains('production mixte indetermine')
    is_ind_ecs = is_ind_ecs | (td001_sys.gen_ecs_lib_final == "indetermine")
    is_ind_ch = td001_sys.gen_ch_lib_final == "indetermine"

    is_pac_ch = td001_sys.gen_ch_lib_final.str.contains('pac') & (
        ~td001_sys.gen_ch_lib_final.str.contains('pac air/air'))

    td001_sys.loc[is_ind_ecs & is_pac_ch, 'gen_ecs_lib_final'] = 'ecs thermodynamique electrique(pac ou ballon)'

    # si pac en releve de chaudiere
    is_ind_ecs = td001_sys.gen_ecs_lib_final.str.contains('production mixte indetermine')
    is_ind_ecs = is_ind_ecs | (td001_sys.gen_ecs_lib_final == "indetermine")
    is_releve = td001_sys.gen_ch_lib_final.str.contains('releve')

    sel = is_releve & is_ind_ecs
    td001_sys.loc[sel, 'gen_ecs_lib_final'] = 'ecs thermodynamique electrique(pac ou ballon)'

    sel = sel & (td001_sys.chaudiere_ch != '')
    td001_sys.loc[sel, 'gen_ecs_lib_final'] = td001_sys.loc[sel, 'gen_ecs_lib_final'] + ' + ' + td001_sys.loc[
        sel, 'chaudiere_ch']

    # ===================================== CHAUDIERE ELEC ET BOIS ================================================

    is_chaudiere_elec = td001_sys.gen_ch_lib_final.str.contains(
        'chaudiere energie indetermine indetermine + chauffage electrique indetermine')

    td001_sys.loc[is_chaudiere_elec, 'gen_ch_lib_final'] = td001_sys.loc[
        is_chaudiere_elec, 'gen_ch_lib_final'].str.replace(
        'chaudiere energie indetermine indetermine + chauffage electrique indetermine', 'chaudiere electrique')

    is_chaudiere_bois = td001_sys.gen_ch_lib_final.str.contains('chaudiere bois')
    is_chaudiere_ind = td001_sys.gen_ch_lib_final.str.contains('chaudiere energie indetermine indetermine')

    td001_sys.loc[is_chaudiere_bois & is_chaudiere_ind, 'gen_ch_lib_final'] = f'chaudiere bois'

    for type_chaudiere in type_chaudiere_mods:
        is_chaudiere_ind = td001_sys.gen_ch_lib_final.str.contains(
            f'chaudiere energie indetermine {type_chaudiere}')

        td001_sys.loc[is_chaudiere_bois & is_chaudiere_ind, 'gen_ch_lib_final'] = f'chaudiere bois'

    is_chaudiere_elec = td001_sys.gen_ecs_lib_final.str.contains(
        'chaudiere energie indetermine indetermine + chauffage electrique indetermine')

    td001_sys.loc[is_chaudiere_elec, 'gen_ecs_lib_final'] = td001_sys.loc[
        is_chaudiere_elec, 'gen_ecs_lib_final'].str.replace(
        'chaudiere energie indetermine indetermine + chauffage electrique indetermine', 'chaudiere electrique')

    is_chaudiere_bois = td001_sys.gen_ecs_lib_final.str.contains('chaudiere bois')
    is_chaudiere_ind = td001_sys.gen_ecs_lib_final.str.contains('chaudiere energie indetermine indetermine')

    td001_sys.loc[is_chaudiere_bois & is_chaudiere_ind, 'gen_ecs_lib_final'] = f'chaudiere bois'

    # for type_chaudiere in type_chaudiere_mods:
    #     is_chaudiere_ind = td001_sys.gen_ecs_lib_final.str.contains(
    #         f'chaudiere energie indetermine {type_chaudiere}')
    #
    #     td001_sys.loc[is_chaudiere_bois, 'gen_ecs_lib_final'] = f'chaudiere bois'

    # ======= Suppression des libéllés chauffage indetermine avec une chaudiere =======================================

    for ener in energie_chaudiere_mods:
        is_chaudiere_ener = td001_sys.gen_ch_lib_final.str.contains(f'chaudiere {ener}')
        for ener_other in energie_chaudiere_mods:
            lib_ind_other = f"chauffage {ener_other} indetermine"
            is_ind_other = td001_sys.gen_ch_lib_final.str.contains(lib_ind_other)

            td001_sys.loc[is_chaudiere_ener & is_ind_other, 'gen_ch_lib_final'] = td001_sys.loc[
                is_chaudiere_ener & is_ind_other, 'gen_ch_lib_final'].str.replace(' \+ ' + lib_ind_other, '',regex=False)

    # affectation de libéllés par défaut en fonction des energies de chauffage et ecs pour les générateurs indéterminés

    # CHAUFFAGE

    ind = td001_sys.gen_ch_lib_final == "indetermine"

    for type_ener in td001_sys.td002_type_energie_ch.dropna().unique():

        ener = td001_sys.td002_type_energie_ch == type_ener

        if type_ener in energie_chaudiere_mods:

            td001_sys.loc[ind & ener, 'gen_ch_lib_final'] = f'chaudiere {type_ener} indetermine'

        elif 'reseau de chaleur' in type_ener:

            td001_sys.loc[ind & ener, 'gen_ch_lib_final'] = f'reseau de chaleur'

        elif 'electricite' == type_ener:
            td001_sys.loc[ind & ener, 'gen_ch_lib_final'] = 'chauffage electrique indetermine'

        else:
            td001_sys.loc[ind & ener, 'gen_ch_lib_final'] = ' + '.join(
                [f'chauffage {el} indetermine' for el in type_ener.split(' + ')])

    for type_ener in td001_sys.td016_type_energie_ch.dropna().unique():

        ener = td001_sys.td016_type_energie_ch == type_ener

        if type_ener in energie_chaudiere_mods:

            td001_sys.loc[ind & ener, 'gen_ch_lib_final'] = f'chaudiere {type_ener} indetermine'

        elif 'reseau de chaleur' in type_ener:

            td001_sys.loc[ind & ener, 'gen_ch_lib_final'] = f'reseau de chaleur'

        elif 'electricite' == type_ener:

            td001_sys.loc[ind & ener, 'gen_ch_lib_final'] = f'chauffage electrique indetermine'

        else:

            td001_sys.loc[ind & ener, 'gen_ch_lib_final'] = ' + '.join(
                [f'chauffage {el} indetermine' for el in type_ener.split(' + ')])

    td001_sys.gen_ch_lib_final = td001_sys.gen_ch_lib_final.str.replace('chauffage electricite', 'chauffage electrique')

    # ECS

    ind = td001_sys.gen_ecs_lib_final == "indetermine"

    for type_ener in td001_sys.td002_type_energie_ecs.dropna().unique():

        ener = td001_sys.td002_type_energie_ecs == type_ener

        if type_ener in energie_chaudiere_mods:

            td001_sys.loc[ind & ener, 'gen_ecs_lib_final'] = f'chaudiere {type_ener} indetermine'

        elif 'reseau de chaleur' in type_ener:

            td001_sys.loc[ind & ener, 'gen_ecs_lib_final'] = f'reseau de chaleur'

        elif 'electricite' == type_ener:
            td001_sys.loc[ind & ener, 'gen_ecs_lib_final'] = 'ecs electrique indetermine'

        else:
            td001_sys.loc[ind & ener, 'gen_ecs_lib_final'] = ' + '.join(
                [f'ecs {el} indetermine' for el in type_ener.split(' + ')])

    for type_ener in td001_sys.td016_type_energie_ecs.dropna().unique():

        ener = td001_sys.td016_type_energie_ecs == type_ener

        if type_ener in energie_chaudiere_mods:

            td001_sys.loc[ind & ener, 'gen_ecs_lib_final'] = f'chaudiere {type_ener} indetermine'

        elif 'reseau de chaleur' in type_ener:

            td001_sys.loc[ind & ener, 'gen_ecs_lib_final'] = f'reseau de chaleur'

        elif 'electricite' == type_ener:

            td001_sys.loc[ind & ener, 'gen_ecs_lib_final'] = f'ecs electrique indetermine'

        else:

            td001_sys.loc[ind & ener, 'gen_ecs_lib_final'] = ' + '.join(
                [f'ecs {el} indetermine' for el in type_ener.split(' + ')])

    td001_sys.gen_ecs_lib_final = td001_sys.gen_ecs_lib_final.str.replace('ecs electricite', 'ecs electrique')

    # ======================================== reorder labels =========================================================

    ordered_ch_labels = ['chauffage solaire'] + list(gen_ch_search_dict_flat.keys()) + ['chauffage autre indetermine',
                                                                                        'indetermine']
    ordered_ecs_labels = list(gen_ecs_search_dict_flat.keys()) + ['ecs autre indetermine', 'indetermine']

    def resort_gen_ch_labels(x):
        try:
            c = pd.Categorical(x.split(' + '), ordered_ch_labels, ordered=True)
            c = ' + '.join(c.sort_values().tolist())
            return c
        except Exception as e:

            raise e

    def resort_gen_ecs_labels(x):
        try:
            c = pd.Categorical(x.split(' + '), ordered_ecs_labels, ordered=True)
            c = ' + '.join(c.sort_values().tolist())
        except Exception as e:

            raise e
        return x

    plus = td001_sys.gen_ch_lib_final.str.contains(' \+ ')

    td001_sys.loc[plus, 'gen_ch_lib_final'] = td001_sys.loc[plus, 'gen_ch_lib_final'].apply(
        lambda x: resort_gen_ch_labels(x))

    plus = td001_sys.gen_ecs_lib_final.str.contains(' \+ ')

    td001_sys.loc[plus, 'gen_ecs_lib_final'] = td001_sys.loc[plus, 'gen_ecs_lib_final'].apply(
        lambda x: resort_gen_ecs_labels(x))

    # ====================================redressement energie ========================================================

    # CHAUFFAGE

    gen_to_energy = dict()

    for k in ordered_ch_labels:
        ener = energie_normalise_ordered
        for ener in reversed(ener):
            if ener in k:
                gen_to_energy[k] = ener
        if 'pac' in k or 'elec' in k:
            gen_to_energy[k] = 'electricite'
        if k not in gen_to_energy:
            gen_to_energy[k] = ''

    gen_to_energy['poele ou insert indetermine'] = 'autre'

    td001_sys['type_energie_ch_from_gen'] = td001_sys.gen_ch_lib_final.copy()
    for k, v in gen_to_energy.items():
        td001_sys['type_energie_ch_from_gen'] = td001_sys['type_energie_ch_from_gen'].str.replace(k, v,regex=False)

    td001_sys['type_energie_ch_from_gen'] = td001_sys['type_energie_ch_from_gen'].apply(
        lambda x: ' + '.join(sorted(list(set([el for el in x.split(' + ') if el != ''])))))

    td001_sys['type_energie_ch_concat'] = ''

    for col_ener in ['type_energie_ch_from_gen', 'type_energie_ch',
                     'mix_energetique_ch', 'td002_type_energie_ch', 'td016_type_energie_ch']:
        is_null = td001_sys[col_ener].isnull()
        td001_sys.loc[~is_null, 'type_energie_ch_concat'] += ' + ' + td001_sys.loc[~is_null, col_ener]

    td001_sys.type_energie_ch_concat = td001_sys.type_energie_ch_concat.str.replace('indetermine', '')

    td001_sys['type_energie_ch_concat'] = td001_sys['type_energie_ch_concat'].apply(
        lambda x: ' + '.join(sorted(list(set([el for el in x.split(' + ') if el != ''])))))

    # Nettoyage des mentions autre lorsqu'un combustible correspondant est présent.
    is_autre = td001_sys.type_energie_ch_concat.str.contains('autre')
    is_autres_ener = td001_sys.type_energie_ch_concat.str.contains('fioul|reseau de chaleur|gpl/butane/propane')
    td001_sys.loc[is_autres_ener & is_autre, 'type_energie_ch_concat'] = td001_sys.loc[
        is_autres_ener & is_autre, 'type_energie_ch_concat'].str.replace('autre', '')

    # Correction des confusions GPL -> GAZ
    bad_corr = td001_sys.type_energie_ch_concat != td001_sys.type_energie_ch_from_gen
    gpl_gaz_mixup = td001_sys.type_energie_ch_concat.str.contains('gpl/butane/propane') & bad_corr
    gpl_gaz_mixup = gpl_gaz_mixup & td001_sys.type_energie_ch_from_gen.str.contains('gaz')
    td001_sys.loc[gpl_gaz_mixup, 'type_energie_ch_concat'] = td001_sys.loc[
        gpl_gaz_mixup, 'type_energie_ch_concat'].str.replace('gpl/butane/propane', 'gaz')

    # Correction des confusions GAZ -> GPL

    bad_corr = td001_sys.type_energie_ch_concat != td001_sys.type_energie_ch_from_gen
    gpl_gaz_mixup = td001_sys.type_energie_ch_from_gen.str.contains('gpl/butane/propane') & bad_corr
    gpl_gaz_mixup = gpl_gaz_mixup & td001_sys.type_energie_ch_concat.str.contains('gaz')
    td001_sys.loc[gpl_gaz_mixup, 'type_energie_ch_concat'] = td001_sys.loc[
        gpl_gaz_mixup, 'type_energie_ch_concat'].str.replace('gaz', 'gpl/butane/propane')

    td001_sys['type_energie_ch_concat'] = td001_sys['type_energie_ch_concat'].apply(
        lambda x: ' + '.join(sorted(list(set([el for el in x.split(' + ') if el != '']))))).str.strip()

    bad_corr = td001_sys.type_energie_ch_concat != td001_sys.type_energie_ch_from_gen
    # S'il reste des incohérences entre système et energie on privilégie alors le type d'energie des générateurs
    td001_sys.loc[bad_corr, 'type_energie_ch_concat'] = td001_sys.loc[bad_corr, 'type_energie_ch_from_gen']
    td001_sys['type_energie_ch'] = td001_sys.type_energie_ch_concat

    # ECS

    gen_to_energy = dict()

    for k in ordered_ecs_labels:
        if k == 'ecs thermodynamique electrique(pac ou ballon)':
            k = 'ecs thermodynamique electrique\(pac ou ballon\)'
        ener = energie_normalise_ordered
        for ener in reversed(ener):
            if ener in k:
                gen_to_energy[k] = ener
        if 'pac' in k or 'elec' in k:
            gen_to_energy[k] = 'electricite'
        if k not in gen_to_energy:
            gen_to_energy[k] = ''

    td001_sys['type_energie_ecs_from_gen'] = td001_sys.gen_ecs_lib_final.copy()
    for k, v in gen_to_energy.items():
        td001_sys['type_energie_ecs_from_gen'] = td001_sys['type_energie_ecs_from_gen'].str.replace(k, v,regex=False)

    td001_sys['type_energie_ecs_from_gen'] = td001_sys['type_energie_ecs_from_gen'].apply(
        lambda x: ' + '.join(sorted(list(set([el for el in x.split(' + ') if el != ''])))))

    td001_sys['type_energie_ecs_concat'] = ''

    for col_ener in ['type_energie_ecs_from_gen', 'type_energie_ecs',
                     'mix_energetique_ecs', 'td002_type_energie_ecs', 'td016_type_energie_ecs']:
        is_null = td001_sys[col_ener].isnull()
        td001_sys.loc[~is_null, 'type_energie_ecs_concat'] += ' + ' + td001_sys.loc[~is_null, col_ener]

    td001_sys.type_energie_ecs_concat = td001_sys.type_energie_ecs_concat.str.replace('indetermine', '')

    td001_sys['type_energie_ecs_concat'] = td001_sys['type_energie_ecs_concat'].apply(
        lambda x: ' + '.join(sorted(list(set([el for el in x.split(' + ') if el != ''])))))

    # Nettoyage des mentions autre lorsqu'un combustible correspondant est présent.
    is_autre = td001_sys.type_energie_ecs_concat.str.contains('autre')
    is_autres_ener = td001_sys.type_energie_ecs_concat.str.contains('fioul|reseau de chaleur|gpl/butane/propane')
    td001_sys.loc[is_autres_ener & is_autre, 'type_energie_ecs_concat'] = td001_sys.loc[
        is_autres_ener & is_autre, 'type_energie_ecs_concat'].str.replace('autre', '')

    # Correction des confusions GPL -> GAZ
    bad_corr = td001_sys.type_energie_ecs_concat != td001_sys.type_energie_ecs_from_gen
    gpl_gaz_mixup = td001_sys.type_energie_ecs_concat.str.contains('gpl/butane/propane') & bad_corr
    gpl_gaz_mixup = gpl_gaz_mixup & td001_sys.type_energie_ecs_from_gen.str.contains('gaz')
    td001_sys.loc[gpl_gaz_mixup, 'type_energie_ecs_concat'] = td001_sys.loc[
        gpl_gaz_mixup, 'type_energie_ecs_concat'].str.replace('gpl/butane/propane', 'gaz')

    # Correction des confusions GAZ -> GPL

    bad_corr = td001_sys.type_energie_ecs_concat != td001_sys.type_energie_ecs_from_gen
    gpl_gaz_mixup = td001_sys.type_energie_ecs_from_gen.str.contains('gpl/butane/propane') & bad_corr
    gpl_gaz_mixup = gpl_gaz_mixup & td001_sys.type_energie_ecs_concat.str.contains('gaz')
    td001_sys.loc[gpl_gaz_mixup, 'type_energie_ecs_concat'] = td001_sys.loc[
        gpl_gaz_mixup, 'type_energie_ecs_concat'].str.replace('gaz', 'gpl/butane/propane')

    td001_sys['type_energie_ecs_concat'] = td001_sys['type_energie_ecs_concat'].apply(
        lambda x: ' + '.join(sorted(list(set([el for el in x.split(' + ') if el != '']))))).str.strip()

    bad_corr = td001_sys.type_energie_ecs_concat != td001_sys.type_energie_ecs_from_gen

    # S'il reste des incohérences entre système et energie on privilégie alors le type d'energie des générateurs
    td001_sys.loc[bad_corr, 'type_energie_ecs_concat'] = td001_sys.loc[bad_corr, 'type_energie_ecs_from_gen']
    td001_sys['type_energie_ecs'] = td001_sys.type_energie_ecs_concat

    lib_final_eq_lib_data = (td001_sys.gen_ch_lib_infer_concat == td001_sys.gen_ch_lib_final).fillna(False)
    
    # INDICATEURS source info
    
    td001_sys['src_gen_ch_lib'] = 'libelle non redresse determine'

    ind_data = td001_sys.gen_ch_lib_infer_concat.str.contains('indetermine').fillna(False)
    not_data = td001_sys.gen_ch_lib_infer_concat.isnull()
    td001_sys.loc[
        lib_final_eq_lib_data & (~ind_data), 'src_gen_ch_lib'] = 'libelle fiable et coherent ft desc et td012'

    is_redresse = td001_sys.gen_ch_lib_final != td001_sys.gen_ch_lib_non_retraite
    td001_sys.loc[is_redresse, 'src_gen_ch_lib'] = 'libelle redresse determine'

    is_ind = td001_sys.gen_ch_lib_final == 'indetermine'

    td001_sys.loc[is_ind, 'src_gen_ch_lib'] = 'libelle indetermine'

    is_ind = td001_sys.gen_ch_lib_final.str.contains('indetermine')

    td001_sys.loc[is_ind, 'src_gen_ch_lib'] = td001_sys.loc[is_ind, 'src_gen_ch_lib'].str.replace(' determine',
                                                                                                    ' partiellement indetermine')

    is_ind = td001_sys.gen_ecs_lib_final.str.contains('indetermine')

    lib_final_eq_lib_data = (td001_sys.gen_ecs_lib_infer_concat == td001_sys.gen_ecs_lib_final).fillna(False)

    td001_sys['src_gen_ecs_lib'] = 'libelle non redresse determine'

    ind_data = td001_sys.gen_ecs_lib_infer_concat.str.contains('indetermine').fillna(False)
    not_data = td001_sys.gen_ecs_lib_infer_concat.isnull()
    td001_sys.loc[
        lib_final_eq_lib_data & (~ind_data), 'src_gen_ecs_lib'] = 'libelle fiable et coherent ft desc et td012'

    is_redresse = td001_sys.gen_ecs_lib_final != td001_sys.gen_ecs_lib_non_retraite
    td001_sys.loc[is_redresse, 'src_gen_ecs_lib'] = 'libelle redresse determine'

    is_ind = td001_sys.gen_ecs_lib_final == 'indetermine'

    td001_sys.loc[is_ind, 'src_gen_ecs_lib'] = 'libelle indetermine'

    is_ind = td001_sys.gen_ecs_lib_final.str.contains('indetermine')

    td001_sys.loc[is_ind, 'src_gen_ecs_lib'] = td001_sys.loc[is_ind, 'src_gen_ecs_lib'].str.replace(' determine',
                                                                                                      ' partiellement indetermine')

    return td001_sys


def calcul_libelle_simplifie(td001_sys):
    td001_sys['gen_ch_lib_final_simp'] = td001_sys['gen_ch_lib_final'].copy()

    ### simplification des dénominations generateurs


    for ener in energie_chaudiere_mods:
        gen_ch_simp_dict[f'chauffage {ener} indetermine'] = f'chaudiere {ener} standard'
        gen_ch_simp_dict[f'chauffage electricite + {ener} indetermine'] = f'chaudiere {ener} standard'
        gen_ch_simp_dict[f'chaudiere {ener} indetermine'] = f'chaudiere {ener} standard'

    for k, v in gen_ch_simp_dict.items():
        td001_sys.gen_ch_lib_final_simp = td001_sys.gen_ch_lib_final_simp.str.replace(k, v,regex=False)
        td001_sys.gen_ch_lib_final_simp = td001_sys.gen_ch_lib_final_simp.str.replace(v + ' \+ ' + v, v,regex=False)
        td001_sys.gen_ch_lib_final_simp = td001_sys.gen_ch_lib_final_simp.str.replace(v + ' \+ ' + v, v,regex=False)

    # ### simplification réseau de chaleur
    # #si réseau de chaleur alors on considère que c'est le système de chauffage du bâtiment
    # is_reseau_ch = td001_sys.gen_ch_lib_final.str.contains('reseau de chaleur')
    # is_not_solaire = ~td001_sys.gen_ch_lib_final.str.contains('solaire')
    # td001_sys.loc[is_reseau_ch&is_not_solaire,'gen_ch_lib_final_simp']='reseau de chaleur'

    ### limiter les combinaisons de systemes à 2

    # is_sup_2_sys=td001_sys.gen_ch_lib_final_simp.str.count(' \+ ')>=2

    # td001_sys.loc[is_sup_2_sys,'gen_ch_lib_final_simp']=td001_sys.loc[is_sup_2_sys,'gen_ch_lib_final_simp'].apply(lambda x:' + '.join(x.split(' + ')[0:2]))

    ### simplification de la mention en releve de chaudiere pour les systèmes thermo

    # on considère les systèmes thermo comme le principal usage.

    td001_sys.gen_ch_lib_final_simp = td001_sys.gen_ch_lib_final_simp.str.replace(' en releve de chaudiere', '')

    ### generateur de chauffage principal et  appoint

    plus = td001_sys.gen_ch_lib_final_simp.str.contains('\+')

    td001_sys['gen_ch_lib_principal'] = td001_sys.gen_ch_lib_final_simp.apply(
        lambda x: [el for el in x.split(' + ')][0])

    td001_sys.loc[plus, 'gen_ch_lib_appoint'] = td001_sys.loc[plus, 'gen_ch_lib_final_simp'].apply(
        lambda x: [el for el in x.split(' + ')][1])

    poele = td001_sys.gen_ch_lib_principal.str.contains('poele')

    td001_sys.loc[plus & poele, 'gen_ch_lib_principal'] = td001_sys.loc[plus & poele, 'gen_ch_lib_final_simp'].apply(
        lambda x: [el for el in x.split(' + ')][1])

    td001_sys.loc[plus & poele, 'gen_ch_lib_appoint'] = td001_sys.loc[plus & poele, 'gen_ch_lib_final_simp'].apply(
        lambda x: [el for el in x.split(' + ')][0])

    # Systeme ECS

    ### simplification des dénominations des generateurs

    td001_sys['gen_ecs_lib_final_simp'] = td001_sys['gen_ecs_lib_final'].copy()



    for ener in energie_chaudiere_mods:
        gen_ecs_simp_dict[f'ecs {ener} indetermine'] = f'chaudiere {ener} standard'
        gen_ecs_simp_dict[f'ecs electricite + {ener} indetermine'] = f'chaudiere {ener} standard'
        gen_ecs_simp_dict[f'production mixte {ener}'] = f'chaudiere {ener} standard'
        gen_ecs_simp_dict[f'chaudiere {ener} indetermine'] = f'chaudiere {ener} standard'

    for k, v in gen_ecs_simp_dict.items():
        td001_sys.gen_ecs_lib_final_simp = td001_sys.gen_ecs_lib_final_simp.str.replace(k, v,regex=False)
        td001_sys.gen_ecs_lib_final_simp = td001_sys.gen_ecs_lib_final_simp.str.replace(v + ' + ' + v, v,regex=False)
        td001_sys.gen_ecs_lib_final_simp = td001_sys.gen_ecs_lib_final_simp.str.replace(v + ' + ' + v, v,regex=False)

    ### generateur ecs principal appoint et solaire

    plus = td001_sys.gen_ecs_lib_final_simp.str.contains('\+')

    td001_sys['gen_ecs_lib_principal'] = td001_sys.gen_ecs_lib_final_simp.apply(
        lambda x: [el for el in x.split(' + ')][0])

    td001_sys.loc[plus, 'gen_ecs_lib_appoint'] = td001_sys.loc[plus, 'gen_ecs_lib_final_simp'].apply(
        lambda x: [el for el in x.split(' + ')][1])

    return td001_sys
