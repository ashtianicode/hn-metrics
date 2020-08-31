import os

class Config(object):
    Debug = True
    Testing = True
    CSRF_ENABLED = True
    SECRET_KEY = ""

class ProductionConfig(Config):
    DEBUG = False

class DevelopmentConfig(Config):
    DEVELOPMENT = True
    DEBUG = True
