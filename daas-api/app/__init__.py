from flask import Flask

from routes.data_ingesting import data_ingesting
from routes.data_loading import data_loading
from routes.data_cleaning import data_cleaning


def create_app():
    app = Flask(__name__)

    with app.app_context():
        app.register_blueprint(data_ingesting, url_prefix="/data-ingesting")
        app.register_blueprint(data_loading, url_prefix="/data-loading")
        app.register_blueprint(data_cleaning, url_prefix="/data-cleaning")


        @app.route("/", methods=["GET"])
        def index():
            return "Welcome to the Diastema DaaS API!", 200


    return app
