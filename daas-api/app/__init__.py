from flask import Flask


def create_app():
    app = Flask(__name__)

    with app.app_context():


        @app.route("/", methods=["GET"])
        def index():
            return "Welcome to the Diastema DaaS API!", 200


    return app
