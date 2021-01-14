import pandas as pd
import numpy as np
from .text_matching_dict import gen_ch_search_dict_flat, reverse_cat_gen_ch, gen_ch_search_dict, reverse_cat_gen_ecs, \
    gen_ecs_search_dict, gen_ecs_search_dict_flat, tr003_desc_to_gen, energie_combustion_mods, priorisation_ecs, \
    energie_chaudiere_mods
from .conversion_normalisation import energie_normalise_ordered
from .td003_td005_text_extraction_processing import extract_td003_ch_variables, extract_td003_ecs_variables, \
    extract_td005_ch_variables, extract_td005_ecs_variables

from .td002_td016_processing import extract_type_energie_from_td002_td016, merge_td002_td016_trtrv


def main_advanced_system_processing(td001_sys_ch, td001, td002, td016,
                                    td001_sys_ecs, td003, td005, td011_p, td012_p, td014_p):
    td001_sys = td001.rename(columns={'id': 'td001_dpe_id'})[['td001_dpe_id', 'tr002_type_batiment_id']].merge(
        td001_sys_ch[['td001_dpe_id',
                      'gen_ch_lib_infer_concat',
                      ]], on='td001_dpe_id',
        how='left')
    td001_sys = td001_sys.merge(td001_sys_ecs[['td001_dpe_id', 'gen_ecs_lib_infer_concat']], on='td001_dpe_id',
                                how='left')

    # ELASTIC SEARCH descriptif et fiches techniques

    gen_ch_lib_ft, type_installation_ch_ft, energie_ch_ft, solaire_ch_ft = extract_td005_ch_variables(td005)

    gen_ch_lib_desc, type_installation_ch_desc, energie_ch_desc, solaire_ch_desc = extract_td003_ch_variables(td003)

    gen_ecs_lib_ft, type_installation_ecs_ft, energie_ecs_ft = extract_td005_ecs_variables(td005)

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



def generate_lib_for_sys_ch_and_ecs(td001_sys, gen_ch_lib_ft, type_installation_ch_ft, energie_ch_ft, solaire_ch_ft,
                                    gen_ch_lib_desc, type_installation_ch_desc, energie_ch_desc, solaire_ch_desc,
                                    gen_ecs_lib_ft, type_installation_ecs_ft, energie_ecs_ft,
                                    gen_ecs_lib_desc, type_installation_ecs_desc, energie_ecs_desc
                                    , td011_p, td012_p, td014_p):
    ######################### assemblage des libéllés extrait des données textes et du modèle de données ###############

    gen_ch_lib = concat_and_sort_gen_ch_lib(gen_ch_lib_desc, gen_ch_lib_ft, td012_p)

    type_installation_ch = concat_and_sort_type_installation_ch_lib(type_installation_ch_desc, type_installation_ch_ft,
                                                                    td011_p)
    energie_ch = concat_and_sort_energie_ch_lib(energie_ch_desc, energie_ch_ft,
                                                td012_p)

    gen_ecs_lib = concat_and_sort_gen_ecs_lib(gen_ecs_lib_desc, gen_ecs_lib_ft, td012_p)

    type_installation_ecs = concat_and_sort_type_installation_ecs_lib(type_installation_ecs_desc,
                                                                      type_installation_ecs_ft,
                                                                      td014_p)
    energie_ecs = concat_and_sort_energie_ecs_lib(energie_ecs_desc, energie_ecs_ft,
                                                  td014_p)

    ################################ amelioration des labels et suppresion des labels indetermine ######################

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

    ###############################################concatenation des libéllés###########################################

    ## libéllé générateur chauffage

    gen_ch_lib = gen_ch_lib.sort_values(by=['td001_dpe_id', 'category', 'label', 'source'])

    # on ne garde que le meilleur libéllé de chaque catégorie
    gen_ch_lib = gen_ch_lib.drop_duplicates(subset=['td001_dpe_id', 'category'])

    # on concatene les libéllés pour avoir un seul libéllé concaténé par dpe
    gen_ch_lib = gen_ch_lib.groupby('td001_dpe_id').label.apply(lambda x: ' + '.join(np.unique(x)))

    gen_ch_lib = gen_ch_lib.to_frame('gen_ch_lib_final')

    td001_sys = td001_sys.merge(gen_ch_lib.reset_index(), on='td001_dpe_id', how='left')

    td001_sys.gen_ch_lib_final = td001_sys.gen_ch_lib_final.fillna('indetermine')

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

    type_installation_ch = type_installation_ch.sort_values(by=['td001_dpe_id', 'is_indetermine',
                                                                'source', 'label'])
    type_installation_ch = type_installation_ch.drop_duplicates(subset=['td001_dpe_id'])

    type_installation_ch = type_installation_ch.set_index('td001_dpe_id').label.to_frame('type_installation_ch')

    td001_sys = td001_sys.merge(type_installation_ch.reset_index(), on='td001_dpe_id', how='left')

    td001_sys.type_installation_ch = td001_sys.type_installation_ch.fillna('indetermine')

    ## concatenation du type d'energie de chauffage

    energie_ch = energie_ch.sort_values(by=['td001_dpe_id', 'label', 'source'])

    energie_ch = energie_ch.drop_duplicates(subset=['td001_dpe_id', 'label'])

    energie_ch = energie_ch.groupby('td001_dpe_id').label.apply(lambda x: ' + '.join(np.unique(x))).to_frame(
        'type_energie_ch')

    td001_sys = td001_sys.merge(energie_ch.reset_index(), on='td001_dpe_id', how='left')

    td001_sys.type_energie_ch = td001_sys.type_energie_ch.fillna('indetermine')

    ## concatenation des libéllés de générateurs d'ECS

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

    # libéllés des generateurs chauffage issues des données textes
    gen_ecs_lib_from_txt = pd.concat([gen_ecs_lib_desc[['label', 'category', 'td001_dpe_id']],
                                      gen_ecs_lib_ft[['label', 'category', 'td001_dpe_id']]], axis=0)

    gen_ecs_lib_from_txt = gen_ecs_lib_from_txt.sort_values(by=['td001_dpe_id', 'category', 'label'])

    gen_ecs_lib_from_txt = gen_ecs_lib_from_txt.drop_duplicates(subset=['category', 'td001_dpe_id'], keep='first')

    # suppression de la dénomination exact qui ne correpond qu'a une méthode de recherche plus rigide
    gen_ecs_lib_from_txt.label = gen_ecs_lib_from_txt.label.str.replace(' exact', '')

    gen_ecs_lib_from_txt['source'] = 'txt'

    # libéllés des générateurs chauffage issus de la table td012
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
    gen_ecs_lib['category'] = pd.Categorical(gen_ecs_lib.category, categories=list(gen_ecs_search_dict.keys()),
                                             ordered=True)
    return gen_ecs_lib


def concat_and_sort_type_installation_ecs_lib(type_installation_ecs_desc, type_installation_ecs_ft, td014_p):
    """

    assemblage des libéllés de type installation de chauffage extraits des fiches techniques/descriptifs et extrait du modèle de donnée affilié 3CL.
    on fait un tri collectif>individuel (si on trouve la mention de termes collectifs on les priorise par rapports aux termes individuels

    -------

    """

    # libéllés des generateurs d'ecs issues des données textes
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

    # libéllés des generateurs chauffage issues des données textes
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


def redressement_td001_sys(td001_sys, td001_ener_from_facture):
    # remove trailing indetermine
    td001_sys['gen_ch_lib_final'] = td001_sys.gen_ch_lib_final.str.replace(' \+ indetermine', '')
    td001_sys['gen_ecs_lib_final'] = td001_sys.gen_ecs_lib_final.str.replace(' \+ indetermine', '')

    td001_sys['gen_ch_lib_non_retraite'] = td001_sys.gen_ch_lib_final.copy()
    td001_sys['gen_ecs_lib_non_retraite'] = td001_sys.gen_ecs_lib_final.copy()

    td001_sys = td001_sys.merge(td001_ener_from_facture, on='td001_dpe_id', how='left')
