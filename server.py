import json
from bokeh.plotting import figure
from bokeh.embed import json_item
from bokeh.sampledata.iris import flowers
import hashlib
import yaml
import datetime
import sys
from utils.db_handler import DBHandler


class MyServer:
    def __init__(self):
        with open("configuration/configuration.yaml") as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)


from flask import Flask, request, abort, jsonify

app = Flask(__name__)
my_server = MyServer()
db_handler = DBHandler()
colormap = {"setosa": "red", "versicolor": "green", "virginica": "blue"}
colors = [colormap[x] for x in flowers["species"]]


def encrypt_string(hash_string):
    sha_signature = hashlib.sha256(hash_string.encode()).hexdigest()
    return sha_signature


def check_authorization(request):
    if "auth" not in request.args or "ts" not in request.args:
        return False
    auth_request = request.args["auth"]
    ts_request = request.args["ts"]
    if len(auth_request) != 64 or len(ts_request) != 10:
        return False
    else:
        curr_ts = int(datetime.datetime.now().timestamp())
        if int(ts_request) < curr_ts - 60000 or int(ts_request) > curr_ts + 100:
            # ts too old (10 minutes) or in the future (?)
            return False
    # print(auth_request, file=sys.stderr)
    # print(ts_request, file=sys.stderr)
    my_hash = encrypt_string(str(ts_request) + my_server.CONFIG["server"]["server_secret_key"])
    # print(my_hash, file=sys.stderr)
    if my_hash != auth_request:
        return False
    return True


def make_plot(x, y):
    p = figure(title="Iris Morphology", sizing_mode="fixed", plot_width=400, plot_height=400)
    p.xaxis.axis_label = x
    p.yaxis.axis_label = y
    p.circle(flowers[x], flowers[y], color=colors, fill_alpha=0.2, size=10)
    return p


@app.errorhandler(401)
def not_authorized(e):
    return jsonify(error=str(e)), 401


@app.errorhandler(400)
def bad_request(e):
    return jsonify(error=str(e)), 400


@app.errorhandler(404)
def not_found(e):
    return jsonify(error=str(e)), 404


@app.route("/plot")
def plot():
    authorized = check_authorization(request)
    if authorized:
        p = make_plot("petal_width", "petal_length")
        return json.dumps(json_item(p, "myplot"))
    else:
        abort(401)
        # return {"status": "not_authorized"}


# @app.route("/")
# def home_page():
#     return "Hello world!"


@app.route("/common_words")
def common_words():
    # this endpoint receives START_DATEas parameter
    authorized = check_authorization(request)
    if authorized:
        if "date" not in request.args:
            abort(400)
        else:
            common_words = db_handler.get_common_words(request.args["date"])
            return json.dumps(common_words)
    else:
        abort(401)


if __name__ == "__main__":
    app.run(host="0.0.0.0")
