import sys
from pathlib import Path
if sys.platform == 'linux':
    path_d = Path('/mnt/d')
else:
    path_d = Path('D://')

from flask import Flask, render_template, session, flash, redirect, \
    url_for, jsonify, send_file, send_from_directory, Response,abort

res_dir = path_d / 'test' / 'base_dpe_geocode'


app = Flask(__name__,template_folder=res_dir/'maps')

@app.route('/home')
def home():
    return 'service available'

@app.route('/map/dept/<dept>')
def render_map_dept(dept):
    return render_template(f'/dept/dpe_{dept}.html')

@app.route('/map/citycode/<citycode>')
def render_map_citycode(citycode):
    return render_template(f'/citycode/dpe_{citycode}.html')

if __name__ == '__main__':

    #app.run(port=8080, host="localhost")
    app.run(port=8080, host="0.0.0.0")
