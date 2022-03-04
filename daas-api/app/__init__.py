from flask import Flask

from routes.data_loading import data_loading


def create_app():
    app = Flask(__name__)

    with app.app_context():
        app.register_blueprint(data_loading, url_prefix="/data-loading")


        @app.route("/", methods=["GET"])
        def index():
            return "Welcome to the Diastema DaaS API!", 200


    return app
