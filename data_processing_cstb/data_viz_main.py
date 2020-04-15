import pandas as pd
import geopandas
import json
import requests
from io import StringIO
import traceback as tb
from pathlib import Path
import numpy as np
import geopandas as gpd
import sys
import requests
import json
from pathlib import Path
from io import StringIO, BytesIO
import pandas as pd
import uuid
import geopandas as gpd
import traceback as tb
import uuid
import contextily as ctx
from shapely.geometry import Point
from matplotlib import pyplot as plt
from folium.plugins import MarkerCluster
import folium
from addr_viz import plot_addr_count_by_communes_dept, plot_addr_count_by_communes_by_type_dept, build_addr_folium_map


def to_gdf_mercator(dpe_geo):
    dpe_geo = dpe_geo.dropna(subset=['latitude', 'longitude'])

    dpe_geo[['latitude', 'longitude']] = dpe_geo[['latitude', 'longitude']].astype(float)

    dpe_geo['geometry'] = dpe_geo[['latitude', 'longitude']].apply(lambda x: Point(x.longitude, x.latitude), axis=1)

    dpe_geo = gpd.GeoDataFrame(dpe_geo, geometry='geometry', crs='epsg:4326')

    dpe_geo = dpe_geo.to_crs(epsg=3857)

    dpe_geo[['result_score', 'latitude', 'longitude']] = dpe_geo[['result_score', 'latitude', 'longitude']].astype(
        float)
    return dpe_geo


def main():
    if sys.platform == 'linux':
        path_d = Path('/mnt/d')
    else:
        path_d = Path('D://')

    res_dir = path_d / 'test' / 'base_dpe_geocode'
    res_dir.mkdir(exist_ok=True, parents=True)

    # path for html maps
    maps_dir = Path(res_dir) / 'maps'
    maps_dir.mkdir(exist_ok=True, parents=True)

    # path for static plots
    plot_dir = Path(res_dir) / 'plot'
    plot_dir.mkdir(exist_ok=True, parents=True)

    final_output_dir = Path(res_dir) / 'final_output'

    for path_geo in final_output_dir.iterdir():
        dept = path_geo.name.split('_')[1]
        with pd.HDFStore(path_geo, 'r') as store:
            print(store.keys())
            dpe_geo = store['td001_geocoded']

        dpe_geo = to_gdf_mercator(dpe_geo)

        # addr count by commune geoplot
        plot_addr_count_by_communes_dept(dpe_geo)
        plt.savefig(plot_dir / f'{dept}_dpe_count.png', bbox_inches='tight')

        # addr count by commune and by type of result geoplot

        plot_addr_count_by_communes_by_type_dept(dpe_geo)
        plt.savefig(plot_dir / f'{dept}_dpe_count_type.png', bbox_inches='tight')

        # FOLIUM INTERACTIVE MAP ADDR

        dpe_geo.classe_consommation_energie = dpe_geo.classe_consommation_energie.fillna('N')
        dpe_geo['result_type_concat_energie'] = dpe_geo[
                                                    'result_type'] + '_' + dpe_geo.classe_consommation_energie.fillna(
            'N')

        color_dpe_dict = {
            'A': 'darkgreen',
            'B': 'green',
            "C": "lightgreen",
            "D": "beige",
            "E": "orange",
            "F": "red",
            "G": "darkred",
            "N": "black"

        }

        latlon_cols = ('latitude', 'longitude')
        icon_prop_dict = {'housenumber': {'color': 'blue',
                                          'icon': 'home'},

                          'street': {'color': 'orange',
                                     'icon': 'road'},
                          'municipality': {'color': 'red',
                                           'icon': 'pushpin'},
                          'locality': {'color': 'green',
                                       'icon': 'picture'},
                          }
        icon_prop_temp = icon_prop_dict.copy()
        for k_ener, color in color_dpe_dict.items():
            for k_type, prop in icon_prop_temp.items():
                prop = prop.copy()
                prop['color'] = color
                icon_prop_dict[k_type + '_' + k_ener] = prop
        # group_col='result_type_concat_energie'
        group_col = 'result_type'

        marker_data = ['surface_habitable', 'tr002_type_batiment_id', 'annee_construction',
                       'classe_consommation_energie',
                       'classe_estimation_ges', 'adresse_concat', 'code_postal', 'code_insee',
                       'result_type',
                       'result_score',
                       'result_label', ]

        a_folium_map = build_addr_folium_map(dpe_geo, marker_data=marker_data, group_col=group_col,
                                             icon_prop_dict=icon_prop_dict, latlon_cols=latlon_cols,
                                             color_col='classe_consommation_energie', color_dict=color_dpe_dict)
        a_folium_map.save(str((maps_dir / f'dpe_{dept}.html').absolute()))

if __name__ == '__main__':
    main()