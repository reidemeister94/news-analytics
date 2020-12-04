from flask import Flask, request
import json
from bokeh.plotting import figure
from bokeh.embed import json_item
from bokeh.sampledata.iris import flowers
import hashlib


app = Flask(__name__)
colormap = {"setosa": "red", "versicolor": "green", "virginica": "blue"}
colors = [colormap[x] for x in flowers["species"]]


def encrypt_string(hash_string):
    sha_signature = hashlib.sha256(hash_string.encode()).hexdigest()
    return sha_signature


def check_authorization(request):
    # Note: Define your check, for instance cookie, session.
    # print(request.args)
    if request.args.get("auth", None) is None:
        return False
    auth = request.args["auth"]
    # my_hash = str(timestamp) + "como>>milano"
    # my_hash = encrypt_string(my_hash)
    # print("=" * 50)
    # print("my hash: " + my_hash)
    # print("user hash: " + user_hash)
    # print("=" * 75)
    if auth == "como>>milano":
        flag = True
    else:
        flag = False
    return flag


def make_plot(x, y):
    p = figure(title="Iris Morphology", sizing_mode="fixed", plot_width=400, plot_height=400)
    p.xaxis.axis_label = x
    p.yaxis.axis_label = y
    p.circle(flowers[x], flowers[y], color=colors, fill_alpha=0.2, size=10)
    return p


@app.route("/plot")
def plot():
    authorized = check_authorization(request)
    if authorized:
        p = make_plot("petal_width", "petal_length")
        return json.dumps(json_item(p, "myplot"))
    else:
        return {"status": "not_authorized"}


if __name__ == "__main__":
    app.run(host="0.0.0.0")
