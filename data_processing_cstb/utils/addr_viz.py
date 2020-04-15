import pandas as pd
import folium
from pathlib import Path
from folium.plugins import MarkerCluster
import seaborn as sns
import contextily as ctx
import matplotlib.pyplot as plt
from shapely.geometry import MultiPoint, MultiPolygon
from ban_utils import get_communes_table

def plot_addr_count_by_communes_dept(df_addr_geo, com_ban_geo=None, figsize=(17, 17), plot_kwargs=None, dept=None):
    if dept is None:
        dept = str(df_addr_geo.code_postal.dropna().apply(lambda x: x[0:2]).value_counts().index[0])

        if plot_kwargs is None:
            plot_kwargs = dict()

        if com_ban_geo is None:
            com_ban_geo = get_communes_table(dept, geometry='contour')
        com_ban_geo = com_ban_geo.to_crs(epsg=3857)

        sns.set_context('talk')
        res_by_city = df_addr_geo.pivot_table(index='result_citycode', columns='result_type', values='result_score',
                                              aggfunc='count')

        com_ban_geo_count = com_ban_geo.merge(
            res_by_city.sum(axis=1).to_frame('addr_count').reset_index().rename(columns={'result_citycode': 'code'}),
            on='code', how='left')

        vmax = com_ban_geo_count.addr_count.quantile(0.95)
        ax = com_ban_geo_count.loc[com_ban_geo_count.addr_count.isnull()].plot(color='grey',
                                                                               alpha=0.6,
                                                                               figsize=figsize,
                                                                               edgecolor='k')

        plot_kwargs_default = dict(alpha=0.6, edgecolor='k', cmap='PuRd', vmax=vmax)

        plot_kwargs_default.update(plot_kwargs)
        plot_kwargs = plot_kwargs_default

        com_ban_geo_count.dropna().plot('addr_count', **plot_kwargs_default, ax=ax, legend=True)
        ctx.add_basemap(ax)
        return ax

def plot_addr_count_by_communes_by_type_dept(df_addr_geo, com_ban_geo_contour=None, com_ban_geo_center=None,
                                             figsize=(17, 17), circle_size_factor=3, dept=None):
    sns.set_context('talk')
    color_palette = sns.color_palette()
    if dept is None:
        dept = str(df_addr_geo.code_postal.dropna().apply(lambda x: x[0:2]).value_counts().index[0])

    if com_ban_geo_contour is None:
        com_ban_geo_contour = get_communes_table(dept, geometry='contour')
    com_ban_geo_contour = com_ban_geo_contour.to_crs(epsg=3857)
    if com_ban_geo_center is None:
        com_ban_geo_center = get_communes_table(dept, geometry='center')
    com_ban_geo_center = com_ban_geo_center.to_crs(epsg=3857)

    res_by_city = df_addr_geo.pivot_table(index='result_citycode', columns='result_type', values='result_score',
                                          aggfunc='count')

    com_ban_geo_count = com_ban_geo_contour.merge(
        res_by_city.sum(axis=1).to_frame('addr_count').reset_index().rename(columns={'result_citycode': 'code'}),
        on='code', how='left')
    ax = com_ban_geo_count.loc[com_ban_geo_count.addr_count.isnull()].plot(color='grey',
                                                                           alpha=0.6,
                                                                           figsize=figsize,
                                                                           edgecolor='k')
    plot_kwargs_default = dict(alpha=0.6, edgecolor='k', cmap='PuRd', vmax=100000000000000000000)

    com_ban_geo_count.dropna().plot('addr_count', **plot_kwargs_default, ax=ax)
    for i, result_type in enumerate(df_addr_geo.result_type.dropna().unique()):

        sel = df_addr_geo.loc[df_addr_geo.result_type == result_type]
        if sel.shape[0] > 0:
            sel = sel.dissolve(by=['result_type', 'result_citycode'], aggfunc='first')

            sel['length'] = sel.geometry.apply(
                lambda x: len(x) if isinstance(x, MultiPoint) else 1) * circle_size_factor
            sel = sel.drop('geometry', axis=1).merge(
                com_ban_geo_center[['code', 'geometry']].rename(columns={'code': 'result_citycode'})
                , on='result_citycode')

            sel.plot(ax=ax, label=result_type, markersize='length', alpha=0.6)

    plt.legend(loc='upper right', borderpad=2, fontsize=20)
    ctx.add_basemap(ax)
    return ax


def build_addr_folium_map(df_addr_geo, group_col, marker_data, icon_prop_dict, latlon_cols=('latitude', 'longitude'), color_dict=None,color_col=None):
    a_folium_map = folium.Map(location=df_addr_geo[list(latlon_cols)].mean(axis=0).tolist(), zoom_start=12)

    locations = dict()
    popups = dict()
    icons = dict()
    # we create 3 groups for layer to be displayed
    for group in df_addr_geo[group_col].unique():
        locations[group] = list()
        popups[group] = list()
        icons[group] = list()

        cluster = df_addr_geo.loc[df_addr_geo[group_col] == group]
        mc = MarkerCluster(name=group, options={"spiderfyOnMaxZoom": True,
                                                "removeOutsideVisibleBounds":True,
                                                "singleMarkerMode":False,
                                                "showCoverageOnHover":True,
                                                 #"disableClusteringAtZoom": 18,
                                               })
        cols = marker_data + ['latitude', 'longitude']
        if color_col is not None:
            cols.append(color_col)
        for el in cluster[cols].to_dict(orient='records'):
            frame_body_template = ''
            for data_name in marker_data:
                frame_body_template += f'<h3>{data_name} : '
                frame_body_template += str(el[data_name]) + '<h3>'
            iframe = folium.IFrame(html=frame_body_template, width=300, height=300)
            popup = folium.Popup(iframe)
            location = [el.get(latlon_cols[0]), el.get(latlon_cols[1])]
            icon_prop = icon_prop_dict[group]
            if color_dict is not None:
                icon_prop.update({"color":color_dict.get(el.get(color_col,None),'black')})
            icon = folium.Icon(**icon_prop)
            locations[group].append(location)
            popups[group].append(popup)
            icons[group].append(icon)
            mc.add_child(folium.Marker(location=location, popup=popup, icon=icon))
        a_folium_map.add_child(mc)
    a_folium_map.add_child(folium.LayerControl())
    return a_folium_map