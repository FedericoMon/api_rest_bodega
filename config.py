# Imports
from decouple import config


class Config():
    pass
#     SECRET_KEY = config('SECRET_KEY')


class DevelopmentConfig(Config):
    DEBUG = True
    ssl_disabled = True

    
config = {
    'development': DevelopmentConfig
}
