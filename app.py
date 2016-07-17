from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy

app = Flask()
db = SQLAlchemy()
db.init_app(app)
