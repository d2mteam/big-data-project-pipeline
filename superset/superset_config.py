import os

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "change-this-secret-key")
SQLALCHEMY_DATABASE_URI = os.getenv("SQLALCHEMY_DATABASE_URI")