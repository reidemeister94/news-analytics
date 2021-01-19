import json
from bokeh.plotting import figure
from bokeh.embed import json_item
from bokeh.sampledata.iris import flowers
import hashlib
import yaml
import datetime
import os
import logging
import sys
from utils.db_handler import DBHandler
from functools import wraps


class MyServer:
    def __init__(self):
        with open("configuration/config_server.yaml") as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        self.allowed_ip = self.CONFIG["allowed_ip"]
        self.api_tokens = self.CONFIG["api_tokens"]
        self.LOGGER = self.__get_logger()

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("ServerLog")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "log/server_log.log"
        if not os.path.isdir("log/"):
            os.mkdir("log/")
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger


from flask import Flask, request, abort, jsonify
from flask_httpauth import HTTPTokenAuth

app = Flask(__name__)
auth = HTTPTokenAuth(scheme="Bearer")
my_server = MyServer()
db_handler = DBHandler()
colormap = {"setosa": "red", "versicolor": "green", "virginica": "blue"}
colors = [colormap[x] for x in flowers["species"]]

### Decorators and error code handlers related functions ###


@auth.verify_token
def verify_token(token):
    # my_server.LOGGER.info(token)
    # my_server.LOGGER.info(my_server.api_tokens)
    if token in my_server.api_tokens:
        return my_server.api_tokens[token]


def check_ip(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        ip_client = request.environ.get("HTTP_X_REAL_IP", request.remote_addr)
        if ip_client in my_server.allowed_ip:
            # my_server.LOGGER.info(
            #     "allowed - {} - {}".format(request.remote_addr, my_server.allowed_ip)
            # )
            return f(*args, **kwargs)
        else:
            return abort(401)

    return wrapped


@app.errorhandler(401)
def not_authorized(e):
    return jsonify(error=str(e)), 401


@app.errorhandler(400)
def bad_request(e):
    return jsonify(error=str(e)), 400


@app.errorhandler(404)
def not_found(e):
    return jsonify(error=str(e)), 404


### Utility functions for handling data to return to the client ###


def make_plot(x, y):
    p = figure(title="Iris Morphology", sizing_mode="fixed", plot_width=400, plot_height=400)
    p.xaxis.axis_label = x
    p.yaxis.axis_label = y
    p.circle(flowers[x], flowers[y], color=colors, fill_alpha=0.2, size=10)
    return p


### Routes ###


@app.route("/plot")
@auth.login_required
@check_ip
def plot():
    p = make_plot("petal_width", "petal_length")
    return json.dumps(json_item(p, "myplot"))


# @app.route("/")
# @auth.login_required
# @check_ip
# def index():
#     return "Hello, {}!".format(auth.current_user())


@app.route("/common_words")
@auth.login_required
@check_ip
def common_words():
    if "date" not in request.args or "lang" not in request.args:
        abort(400)
    else:
        common_words = db_handler.get_common_words(request.args["date"], request.args["lang"])
        if common_words is not None:
            return json.dumps(common_words)
        else:
            return "Something went wrong"


if __name__ == "__main__":
    app.run(host="0.0.0.0")
