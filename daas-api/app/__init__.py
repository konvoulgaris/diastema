from flask import Flask


def create_app() -> Flask:
    app = Flask(__name__)


    @app.route("/", methods=["GET"])
    def _():
        return "Welcome to the Diastema DaaS API!", 200


    return app
