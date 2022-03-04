from app import create_app

wsgi = create_app()

if __name__ == "__main__":
    wsgi.run("0.0.0.0", 5000, True)
