import json
from bokeh.models.annotations import Tooltip
from bokeh.models.tools import HoverTool
from bokeh.plotting import figure
from bokeh.embed import json_item, components
from bokeh.models import CustomJS, ColumnDataSource, CDSView, IndexFilter
from bokeh.models.widgets import DateRangeSlider
from bokeh.layouts import column, row
from bokeh.sampledata.iris import flowers
from dateutil.relativedelta import relativedelta
from datetime import date
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
from waitress import serve

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


# def make_plot_flower(x, y):
#     p = figure(title="Iris Morphology", sizing_mode="fixed", plot_width=400, plot_height=400)
#     p.xaxis.axis_label = x
#     p.yaxis.axis_label = y
#     p.circle(flowers[x], flowers[y], color=colors, fill_alpha=0.2, size=10)
#     return p


### Routes ###


@app.route("/plot_articles")
@auth.login_required
@check_ip
def plot_articles():
    if "date" not in request.args or "lang" not in request.args:
        abort(400)
    else:

        reduced_articles = db_handler.get_reduced_articles(
            request.args["date"], request.args["lang"]
        )

        # my_server.LOGGER.info("data collected")
        if reduced_articles is not None:

            source = ColumnDataSource(reduced_articles)

            hover = HoverTool(
                tooltips=[("title", "@title"), ("date", "@date{%F}"),],
                formatters={"@date": "datetime",},
            )

            s_date = datetime.datetime.strptime(request.args["date"], "%Y-%m")
            e_date = s_date + relativedelta(months=1) - datetime.timedelta(days=1)

            date_range_slider = DateRangeSlider(
                value=(
                    date(s_date.year, s_date.month, s_date.day),
                    date(e_date.year, e_date.month, e_date.day),
                ),
                start=date(s_date.year, s_date.month, s_date.day),
                end=date(e_date.year, e_date.month, e_date.day),
            )

            filter = IndexFilter(indices=[])

            callback = CustomJS(
                args=dict(source=source, filter=filter),
                code="""
                    var date_range_low = new Date(cb_obj.value[0])
                    var date_range_high = new Date(cb_obj.value[1])
                    date_range_low.setHours(0,0,0,0)
                    date_range_high.setHours(0,0,0,0)
                    const indices = []
                    for (var i=0; i < source.get_length(); i++) {
                        
                        var date_range_data = new Date(source.data.date[i])

                        if (date_range_data >= date_range_low && date_range_data <= date_range_high) {
                            indices.push(i)
                            break
                        }
                    }
                    console.log(indices.length)

                    filter.indices = indices
                    source.change.emit()
                """,
            )

            date_range_slider.js_on_change("value", callback)

            view = CDSView(source=source, filters=[filter])

            plot = figure(plot_width=600, plot_height=600, tools=[hover], title="Articles",)

            plot.scatter(size=8, color="blue", alpha=0.5, source=source, view=view)

            layout = column(plot, column(date_range_slider),)

            # Testing the results
            script, div = components(layout)

            my_server.LOGGER.info(script)
            my_server.LOGGER.info(div)

            response = app.response_class(
                response=json.dumps(json_item(plot, "myplot")), mimetype="application/json"
            )

            return response
        else:
            my_server.LOGGER.info("Something went wrong")
            return "Something went wrong"


# @app.route("/plot_flower")
# @auth.login_required
# @check_ip
# def plot():
#     p = make_plot_flower("petal_width", "petal_length")

#     response = app.response_class(
#         response=json.dumps(json_item(p, "myplot")), mimetype="application/json"
#     )
#     return response


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
            response = app.response_class(
                response=json.dumps(common_words), mimetype="application/json"
            )
            return response
        else:
            return "Something went wrong"


if __name__ == "__main__":
    serve(app, host="0.0.0.0", url_scheme="https")
