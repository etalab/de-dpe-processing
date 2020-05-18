import seaborn as sns

periode_construction_reglementaire_dict = {
    'avant 1948': (0, 1948),
    'de 1949 à 1974': (1949, 1974),
    'de 1975 à 1981': (1975, 1981),
    'de 1982 à 1987': (1982, 1987),
    'de 1988 à 1999': (1988, 1999),
    'de 2000 à 2005': (2000, 2005),
    "de 2006 à 2012": (2006, 2011),
    "de 2012 à aujourd'hui": (2012, 2100),
}

classe_dpe_valides = ['A', 'B', 'C', 'D', 'E', 'F', 'G']

intervalle_classe_energie = {'A': (0, 51),
                             'B': (51, 91),
                             'C': (91, 151),
                             'D': (151, 231),
                             'E': (231, 331),
                             'F': (331, 451),
                             'G': (451, 1000), }
intervalle_classe_ges = {'A': (0, 6),
                         'B': (6, 11),
                         'C': (11, 21),
                         'D': (21, 36),
                         'E': (36, 56),
                         'F': (56, 80),
                         'G': (80, 500), }
eps = 1e-7
for k, v in intervalle_classe_energie.items():
    intervalle_classe_energie[k] = (v[0], v[1] - eps)
eps = 1e-7
for k, v in intervalle_classe_ges.items():
    intervalle_classe_ges[k] = (v[0], v[1] - eps)

dpe_color_palette = sns.color_palette('RdYlGn', 7)
dpe_color_palette.reverse()
dpe_color_palette.append((0.9, 0.9, 0.9))
