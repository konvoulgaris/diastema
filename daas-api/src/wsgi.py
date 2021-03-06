from flask import Flask

from routes.data_ingesting import data_ingesting
from routes.data_loading import data_loading
from routes.data_cleaning import data_cleaning
from routes.data_sink import data_sink
from routes.join import join


def create_app():
    app = Flask(__name__)

    with app.app_context():
        app.register_blueprint(data_ingesting, url_prefix="/data-ingesting")
        app.register_blueprint(data_cleaning, url_prefix="/data-cleaning")
        app.register_blueprint(data_sink, url_prefix="/data-sink")
        app.register_blueprint(join, url_prefix="/join")


        @app.route("/", methods=["GET"])
        def index():
            return "Welcome to the Diastema DaaS API!", 200


    return app

wsgi = create_app()

if __name__ == "__main__":
    wsgi.run("0.0.0.0", 5000, True)
