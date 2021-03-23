#! /usr/bin/python3
import sys
import csv
import json
import re
import gzip

# modules installés par pip
import requests
import sqlite3
import marshal
import unidecode
import random

# modules locaux
from normadresse.normadresse import abrev

csv_delimiter = '|'

score_min = 0.30

# URL à appeler pour géocodage BAN, BANO et POI OSM
#addok_ban = ['http://localhost:7878/search/','http://localhost:7879/search/','http://localhost:7880/search/','http://localhost:7881/search/','http://localhost:7882/search/','http://localhost:7883/search/','http://localhost:7884/search/','http://localhost:7885/search/','http://localhost:7886/search/','http://localhost:7887/search/']
#addok_poi = 'http://localhost:7830/search'
addok_ban = ['http://localhost:5878/search/', 'http://localhost:5879/search/', 'http://localhost:5880/search/', 'http://localhost:5881/search/', 'http://localhost:5882/search/', 'http://localhost:5883/search/', 'http://localhost:5884/search/', 'http://localhost:5886/search/']
addok_poi = 'http://localhost:5830/search/'

geocode_count = 0

#Certains champs sont des NaN ou des \\N, cette fonction les remplace par une string vide
def emptystring_if_na(value) :
    if (value == value) and (value not in ['\\N', 'Non communiqué', 'Non communiquée', 'non communiqué', 'non communiquée']) :
        return value
    else :
        return ''

# effecture une req. sur l'API de géocodage
def geocode(api, params, l4):
    params['autocomplete'] = 0
    params['q'] = params['q'].strip()
    try:
        r = requests.get(api, params)
        j = json.loads(r.text)
        global geocode_count
        geocode_count += 1
        if 'features' in j and len(j['features']) > 0:
            j['features'][0]['l4'] = l4
            j['features'][0]['geo_l4'] = ''
            j['features'][0]['geo_l5'] = ''
            if api != addok_poi:
                # regénération lignes 4 et 5 normalisées
                name = j['features'][0]['properties']['name']

                ligne4 = re.sub(r'\(.*$', '', name).strip()
                ligne4 = re.sub(r',.*$', '', ligne4).strip()
                ligne5 = ''
                j['features'][0]['geo_l4'] = abrev(ligne4).upper()
                if '(' in name:
                    ligne5 = re.sub(r'.*\((.*)\)', r'\1', name).strip()
                    j['features'][0]['geo_l5'] = abrev(ligne5).upper()
                if ',' in name:
                    ligne5 = re.sub(r'.*,(.*)', r'\1', name).strip()
                    j['features'][0]['geo_l5'] = abrev(ligne5).upper()
                # ligne 4 et 5 identiques ? on supprime la 5
                if j['features'][0]['geo_l5'] == j['features'][0]['geo_l4']:
                    j['features'][0]['geo_l5'] = ''
            return(j['features'][0])
        else:
            return(None)
    except:
        print(json.dumps({'action': 'erreur', 'api': api,
                         'params': params, 'l4': l4}))
        return(None)


def trace(txt):
    if False:
        print(txt)

#Ouverture des CSV et connexion aux bases SQL de stockage des géocodages déjà réalisés
if len(sys.argv) > 2:
    stock = False
    sirene_csv = csv.reader(open(sys.argv[1], 'r'), #NBED: previously encoding='iso8859-1'
                            delimiter=csv_delimiter)
    sirene_geo = csv.writer(gzip.open(sys.argv[2], 'wt', compresslevel=9), delimiter=csv_delimiter)
    conn = None
    if len(sys.argv) > 3:
        print(sys.argv[3])
        conn = sqlite3.connect(sys.argv[3])
        conn.execute('''CREATE TABLE IF NOT EXISTS cache_addok (adr text, geo text,
                     score numeric)''')
        conn.execute('CREATE INDEX IF NOT EXISTS cache_addok_adr ON cache_addok (adr)')  # noqa
        conn.execute('DELETE FROM cache_addok WHERE score<0.7')
else:
    stock = True
    sirene_csv = csv.reader(open(sys.argv[1], 'r'), delimiter=csv_delimiter)
    sirene_geo = csv.writer(open('geo-'+sys.argv[1], 'w'), delimiter=csv_delimiter)
    conn = sqlite3.connect('cache_geo/cache_addok_'+sys.argv[1]+'.db')
    conn.execute('''CREATE TABLE IF NOT EXISTS cache_addok (adr text, geo text,
                 score numeric)''')
    conn.execute('CREATE INDEX IF NOT EXISTS cache_addok_adr ON cache_addok (adr)')  # noqa
    conn.execute('DELETE FROM cache_addok WHERE score<0.7')

# chargement de la liste des communes et lat/lon
communes = csv.DictReader(open('communes-plus-20140630.csv', 'r'))
commune_insee = {}
for commune in communes:
    commune_insee[commune['\ufeffinsee']] = {'lat': round(float(commune['lon_centro']), 6),
                                             'lon': round(float(commune['lat_centro']), 6)}

# chargement des changements de codes INSEE
histo = csv.DictReader(open('histo_depcom.csv', 'r'))
histo_depcom = {}
for commune in histo:
    histo_depcom[commune['DEPCOM']] = commune

header = None
ok = 0
total = 0
cache = 0
numbers = re.compile('(^[0-9]*)')
stats = {'action': 'progress', 'housenumber': 0, 'interpolation': 0,
         'street': 0, 'locality': 0, 'municipality': 0, 'vide': 0,
         'townhall': 0, 'poi': 0, 'fichier': sys.argv[1]}
score_total = 0
score_count = 0
score_variance = 0

# regexp souvent utilisées --> A garder DPE ? NBED
ccial = r'((C|CTRE|CENTRE|CNTRE|CENT|ESPACE) (CCIAL|CIAL|COM|COMM|COMMERC|COMMERCIAL)|CCR|C\.CIAL|C\.C|CCIAL|CC)'  # noqa

#Lecture du header du CSV initial, ajout des colonnes de géocodage et écriture dans le CSV géocodé
header = next(sirene_csv)
header += ['longitude',
          'latitude',
          'geo_score',
          'geo_type',
          'geo_adresse',
          'geo_id',
          'geo_ligne',
          'geo_l4',
          'geo_l5']
header += ['ligne4']
sirene_geo.writerow(header)

#Index des header utilisés pour le géocodage
numvoie_idx = header.index('numero_rue')
typvoie_idx = header.index('type_voie')
libvoie_idx = header.index('nom_rue')
depcom_idx = header.index('code_insee_commune_actualise')

test = 0
test2 = 0
for et in sirene_csv:
    ban_number = random.randint(0,len(addok_ban)-1)
    # on ne tente pas le géocodage des adresses hors de France
    if et[depcom_idx] == '' or re.match(r'^(978|98|99)',et[depcom_idx]):
        row = et+['', '', 0, '', '', '', '', '', '']
        sirene_geo.writerow(row+[''])
    else:
        total = total + 1
        
        #Eléments de l'adresse géographique
        numvoie = emptystring_if_na(et[numvoie_idx])
        typvoie = emptystring_if_na(et[typvoie_idx]).replace('0','') #Some type_voie are 0 for no value
        libvoie = emptystring_if_na(et[libvoie_idx])
        
        #Quelques cas où numvoie == libvoie (adresse complète dans les deux)
        if numvoie == libvoie :
            numvoie = ''

        # code INSEE de la commune
        depcom = emptystring_if_na(et[depcom_idx])
        
        if depcom != '' and depcom < '97000' and depcom in histo_depcom: # code insee inconnu ?
            #if libvoie != '':
            #    libvoie = libvoie + " " + histo_depcom[depcom]['NCC']
            depcom = histo_depcom[depcom]['POLE']

        # élimination des lieux dits des libellés
        #if typvoie.lower().strip() in ['lieu dit', 'lieudit', 'lieu-dit']:
        #    typvoie = ''
        
        #Construction de l'adresse géographique
        ligne4 = ('%s %s %s' % (numvoie, typvoie, libvoie)).strip()
        
        #Enlevons les précisions entre parenthèses dans certaines adresses
        if '(' in ligne4 :
            ligne4 = re.sub(r'\([^)]*\)', '', ligne4).strip()
            
        #ON RETIRE LES INFOS SUPERFLUES QUI GÊNENT LE GEOCODAGE
        regexp_rdc = r'(?i)(rdc|rez de chaussée|rez de chaussee) ?(droite|droit|gauche)?'
        regexp_etage1 = r'(?i)(étage|etage) ?(n°)? ?[0-9]+ ?(à|a)? ?(droite|droit|gauche)?'
        regexp_etage2 = r'(?i)(au)? ?[0-9]+(i)?(ere|ère|ére|er|eme|ème|éme|è|e) ?(étage|etage)? ?(à|a)? ?(droite|droit|gauche)?'
        regexp_lot = r'(?i)(lot) ?(n°)? ?[0-9]+'
        regexp_lots = r'(?i)(lots) ?(n°)? ?[0-9]+ ?(et|-)? ?[0-9]+'
        regexp_appt = r'(?i)(appartement|appart|appt|app|apt)( |\.){1,}(n°)?\S+'
        regexp_bat = r'(?i)(batiment|bâtiment|bât|bat)( |\.){1,}(n°)?\S+'
        regexp_porte1 = r'(?i)(porte|pte) ?(n°)? ?([0-9]+|[A-Z]{1} ) ?(à|a)? ?(droite|droit|gauche)?'
        regexp_porte2 = r'(?i)[0-9]+(i)?(ere|ère|ére|er|eme|ème|éme|è|e) ?(porte|pte) ?(à|a)? ?(droite|droit|gauche)?'
        regexp_maison = r'(?i)(maison) ?(n°)? ?[0-9]+ ?(à|a)? ?(droite|droit|gauche)?'
        regexp_dpenum = r'(?i)dpe(_)+[0-9]+_'
        regexp_bignumstart = r'^[0-9]{6,}' #On enlève à partir de 6 chiffres
        regexp_ref = r'(?i)Rèf. : ([0-9]|\.)+'
        regexp_leftright = '(droite|droit|gauche)?$' #Si jamais il reste un droite/gauche à la fin
        #Ajouter une regexp pour les résidences ?
        
        ligne4 = re.sub(regexp_rdc, '', ligne4).strip(' _-\:/",')
        ligne4 = re.sub(regexp_etage1, '', ligne4).strip(' _-\:/",')
        ligne4 = re.sub(regexp_etage2, '', ligne4).strip(' _-\:/",')
        ligne4 = re.sub(regexp_lot, '', ligne4).strip(' _-\:/",')
        ligne4 = re.sub(regexp_lots, '', ligne4).strip(' _-\:/",')
        ligne4 = re.sub(regexp_appt, '', ligne4).strip(' _-\:/",')
        ligne4 = re.sub(regexp_bat, '', ligne4).strip(' _-\:/",')
        ligne4 = re.sub(regexp_porte1, '', ligne4).strip(' _-\:/",')
        ligne4 = re.sub(regexp_porte2, '', ligne4).strip(' _-\:/",')
        ligne4 = re.sub(regexp_maison, '', ligne4).strip(' _-\:/",')
        ligne4 = re.sub(regexp_dpenum, '', ligne4).strip(' _-\:/",')
        ligne4 = re.sub(regexp_bignumstart, '', ligne4).strip(' _-\:/",')
        ligne4 = re.sub(regexp_ref, '', ligne4).strip(' _-\:/",')
        ligne4 = re.sub(regexp_leftright, '', ligne4).strip(' _-\:/",')
        
            
        #Enlevons les "n°" en fin d'adresse qui apparaissent parfois (num de bâtiment etc)
        if '°' in ligne4 :
            ligne4 = re.sub(r'(?i)n° ?[0-9]+ *$','', ligne4).strip()
        
        #Redétermination des éléments séparés
        try : #Si indice de répétition présent
            indrep = re.search(r'(?i)^[0-9]+ ?(BIS|TER|QUATER|QUINQUIES|B|T|Q|C)', ligne4)[1].strip()
            numvoie = re.search(r'(?i)^[0-9]+ ?(BIS|TER|QUATER|QUINQUIES|B|T|Q|C)', ligne4)[0].replace(indrep,'').strip()
            typlibvoie = re.sub(r'(?i)^[0-9]+ ?(BIS|TER|QUATER|QUINQUIES|B|T|Q|C)', '', ligne4).strip()
        except TypeError :
            indrep = ''
            try :
                numvoie = re.search(r'^[0-9]+', ligne4)[0].strip()
                typlibvoie = re.sub(r'^[0-9]+', '', ligne4).strip()
            except TypeError :
                numvoie = ''
                typlibvoie = ligne4
        
        #Compléments d'adresse à ajouter ? On a batiment/escalier/etage/porte, ne devraient pas être utiles pour le géocoding. SIRENE: ajoutés à la fin pour faire ligne4D...
        
        #On tente d'aller chercher dans la base si l'adresse a déjà été géocodée, sinon on se sert des Addok
        try:
            cursor = conn.execute('SELECT * FROM cache_addok WHERE adr=?',
                                  ('%s|%s' % (depcom, ligne4), ))
            g = cursor.fetchone()
        except:
            g = None
        if g is not None:
            test = test + 1
            source = marshal.loads(g[1])
            cache = cache+1
        else:
            test2 = test2 + 1
            trace('%s' % (ligne4))

            # Géocodage BAN (avec la ligne4 construite ci-dessus dans un premier temps)
            ban = None
            if ligne4 != '':
                ban = geocode(addok_ban[ban_number], {'q': ligne4, 'citycode': depcom, 'limit': '1'}, 'G')

            if ban is not None:
                ban_score = ban['properties']['score']
                ban_type = ban['properties']['type']
                if ['village', 'town', 'city'].count(ban_type) > 0:
                    ban_type = 'municipality'
            else:
                ban_score = 0
                ban_type = ''


            # Choix de la source
            source = None
            score = 0

            #Si on trouve dans BAN, on choisit ce qu'on a trouvé au-dessus comme source, sinon on essaye de faire une interpolation entre les deux numéros voisins (si possible)
            if numvoie != '':
                numvoie_int = int(numvoie)
                if (ban_type == 'housenumber' and ban_score > score_min):
                    source = ban
                    score = ban['properties']['score']
                elif ban is None or ban_type == 'street' and int(numvoie_int) > 2:
                    ban_avant = geocode(addok_ban[ban_number], {'q': '%s %s' % (int(numvoie_int)-2, typlibvoie), 'citycode': depcom, 'limit': '1'}, 'G')
                    ban_apres = geocode(addok_ban[ban_number], {'q': '%s %s' % (int(numvoie_int)+2, typlibvoie), 'citycode': depcom, 'limit': '1'}, 'G')
                    if ban_avant is not None and ban_apres is not None:
                        if (ban_avant['properties']['type'] == 'housenumber' and
                           ban_apres['properties']['type'] == 'housenumber' and
                           ban_avant['properties']['score'] > score_min and
                           ban_apres['properties']['score'] > score_min):
                            source = ban_avant
                            score = (ban_avant['properties']['score']+ban_apres['properties']['score'])/2
                            source['geometry']['coordinates'][0] = round((ban_avant['geometry']['coordinates'][0]+ban_apres['geometry']['coordinates'][0])/2,6)
                            source['geometry']['coordinates'][1] = round((ban_avant['geometry']['coordinates'][1]+ban_apres['geometry']['coordinates'][1])/2,6)
                            source['properties']['score'] = score
                            source['properties']['type'] = 'interpolation'
                            source['properties']['id'] = ''
                            source['properties']['label'] = numvoie + ban_avant['properties']['label'][len(ban_avant['properties']['housenumber']):]

            # On essaye sans l'indice de répétition (BIS, TER qui ne correspond pas ou qui manque en base)
            if source is None and ban is None and indrep != '':
                trace('supp. indrep BAN : %s %s' % (numvoie, typlibvoie))
                addok = geocode(addok_ban[ban_number], {'q': '%s %s' % (numvoie, typlibvoie), 'citycode': depcom, 'limit': '1'}, 'G')
                if addok is not None and addok['properties']['type'] == 'housenumber' and addok['properties']['score'] > score_min:
                    addok['properties']['type'] = 'interpolation'
                    source = addok
                    trace('+ ban  L4-indrep')

            # Pas trouvé ? On décide de conserver la rue renvoyée par le géocodage initial s'il existe
            if source is None and typlibvoie != '':
                if ban_type == 'street' and ban_score > score_min:
                    source = ban
                    score = ban['properties']['score']

            # Pas trouvé ? On cherche à trouver une rue en enlevant numvoie
            if source is None and numvoie != '':
                trace('supp. numvoie : %s %s' % (numvoie, typlibvoie))
                addok = geocode(addok_ban[ban_number], {'q': typlibvoie, 'citycode': depcom, 'limit': '1'}, 'G')
                if addok is not None and addok['properties']['type'] == 'street' and addok['properties']['score'] > score_min:
                    source = addok
                    trace('+ ban  L4-numvoie')
            
            # Toujours pas trouvé ? Tout type accepté dans le géocodage initial
            if source is None:
                if ban_score > score_min:
                    source = ban

            # Vraiment toujours pas trouvé comme adresse ?
            # On cherche dans les POI OpenStreetMap...
            if source is None:
                # Mairies et Hôtels de Ville...
                if ['MAIRIE','LA MAIRIE','HOTEL DE VILLE'].count(typlibvoie.upper()) > 0:
                    poi = geocode(addok_poi, {'q': 'hotel de ville', 'poi': 'townhall', 'citycode': depcom, 'limit': '1'}, 'G')
                    if poi is not None and poi['properties']['score'] > score_min:
                        source = poi
                # Gares...
                elif ['GARE', 'GARE SNCF', 'LA GARE'].count(typlibvoie.upper()) > 0:
                    poi = geocode(addok_poi, {'q': 'gare', 'poi': 'station', 'citycode': depcom, 'limit': '1'}, 'G')
                    if poi is not None and poi['properties']['score'] > score_min:
                        source = poi
                # Centres commerciaux...
                elif re.match(ccial, typlibvoie.upper()) is not None:
                    poi = geocode(addok_poi, {'q': re.sub(ccial, '\1 Galerie Marchande', typlibvoie), 'poi': 'mall', 'citycode': depcom, 'limit': '1'}, 'G')
                    if poi is not None and poi['properties']['score'] > 0.5:
                        source = poi
                elif re.match(ccial, typlibvoie.upper()) is not None:
                    poi = geocode(addok_poi, {'q': re.sub(ccial, '\1 Centre Commercial', typlibvoie), 'citycode': depcom, 'limit': '1'}, 'G')
                    if poi is not None and poi['properties']['score'] > 0.5:
                        source = poi
                # Aéroports et aérodromes...
                elif re.match(r'(AEROPORT|AERODROME)', typlibvoie.upper()) is not None:
                    poi = geocode(addok_poi, {'q': typlibvoie, 'poi': 'aerodrome', 'citycode': depcom, 'limit': '1'}, 'G')
                    if poi is not None and poi['properties']['score'] > score_min:
                        source = poi
                elif re.match(r'(AEROGARE|TERMINAL)', typlibvoie.upper()) is not None:
                    poi = geocode(addok_poi, {'q': re.sub(r'(AEROGARE|TERMINAL)', '', typlibvoie)+' terminal', 'poi': 'terminal', 'citycode': depcom, 'limit': '1'}, 'G')
                    if poi is not None and poi['properties']['score'] > score_min:
                        source = poi

                # recherche tout type de POI à partir de l'adresse
                if source is None:
                    poi = geocode(addok_poi, {'q': ligne4,
                                              'citycode': depcom,
                                              'limit': '1'}, 'G')
                    if poi is not None and poi['properties']['score'] > 0.7:
                        source = poi
                
                # recherche tout type de POI à partir du type et libellé de voie
                if source is None:
                    poi = geocode(addok_poi, {'q': typlibvoie,
                                              'citycode': depcom,
                                              'limit': '1'}, 'G')
                    if poi is not None and poi['properties']['score'] > 0.7:
                        source = poi

                if source is not None:
                    if source['properties']['poi'] != 'yes':
                        source['properties']['type'] = source['properties']['type']+'.'+source['properties']['poi']
                    print(json.dumps({'action': 'poi', 'adr_insee': depcom,
                                      'adr_texte': typlibvoie, 'poi': source},
                                     sort_keys=True))

            if source is not None and score == 0:
                score = source['properties']['score']

            # on conserve le résultat dans le cache sqlite
            if conn:
                key = ('%s|%s' % (depcom, ligne4))
                cursor = conn.execute('INSERT INTO cache_addok VALUES (?,?,?)',
                                      (key, marshal.dumps(source), score))

        #Si toujours rien, on met la localisation de la ville
        if source is None:
            # attention latitude et longitude sont inversées dans le fichier
            # CSV et donc la base sqlite
            row = et+['', '', 0, '', '', '', '', '', '']
            try:
                row = et+[commune_insee[depcom]['lon'],
                          commune_insee[depcom]['lat'],
                          0, 'municipality', '', commune_insee[i], '', '', '']
                if ligne4.strip() != '':
                    if ['CHEF LIEU', 'CHEF-LIEU',
                                          'LE CHEF LIEU', 'LE CHEF-LIEU',
                                          'BOURG', 'LE BOURG', 'AU BOURG',
                                          'VILLAGE', 'AU VILLAGE',
                                          'LE VILLAGE'].count(typlibvoie.upper()) > 0:
                        stats['locality'] += 1
                        ok += 1
                    else:
                        stats['municipality'] += 1
                        print(json.dumps({'action': 'manque',
                                          'siret': et[0]+et[1],
                                          'adr_comm_insee': depcom,
                                          'adr_texte': ligne4.strip()},
                                         sort_keys=True))
                else:
                    stats['vide'] += 1
                    ok += 1
            except:
                pass
            sirene_geo.writerow(row+[ligne4])
        else:
            ok += 1
            if ['village', 'town', 'city'].count(source['properties']['type']) > 0:
                source['properties']['type'] = 'municipality'
            stats[re.sub(r'\..*$', '', source['properties']['type'])] += 1
            sirene_geo.writerow(et+[source['geometry']['coordinates'][0],
                                    source['geometry']['coordinates'][1],
                                    round(source['properties']['score'], 2),
                                    source['properties']['type'],
                                    source['properties']['label'],
                                    source['properties']['id'],
                                    source['l4'] if 'l4' in source else '',
                                    source['geo_l4'] if 'geo_l4' in source else '',
                                    source['geo_l5'] if 'geo_l5' in source else '',
                                    ligne4
                                   ])
            if 'score' in source['properties']:
                score_count = score_count + 1
                score_total = score_total + source['properties']['score']
                if score_count > 100:
                    score_variance = score_variance + (source['properties']['score'] - score_total / score_count) ** 2

        if total % 1000 == 0:
            stats['geocode_cache'] = cache
            stats['count'] = total
            stats['geocode_count'] = geocode_count
            if total>0:
                stats['efficacite'] = round(100*ok/total, 2)
            if score_count > 0:
                stats['geocode_score_avg'] = score_total / score_count
            if score_count > 100:
                stats['geocode_score_variance'] = score_variance / (score_count-101)
            print(json.dumps(stats, sort_keys=True))
            if conn:
                conn.commit()

stats['geocode_cache'] = cache
stats['count'] = total
stats['geocode_count'] = geocode_count
stats['action'] = 'final'
if total>0:
    stats['efficacite'] = round(100*ok/total, 2)
if score_count > 0:
    stats['geocode_score_avg'] = score_total / score_count
if score_count > 100:
    stats['geocode_score_variance'] = score_variance / (score_count-101)
print(json.dumps(stats, sort_keys=True))
if conn:
    conn.commit()
    
#print(stats)
