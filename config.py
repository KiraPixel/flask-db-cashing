import os

SQLALCHEMY_DATABASE_URL = os.getenv('SQLALCHEMY_DATABASE_URL', 'sqlite:///default.db')

CESAR_USERNAME = os.getenv('CESAR_USERNAME', 'default_username')
CESAR_PASSWORD = os.getenv('CESAR_PASSWORD', 'default_password')

WIALON_TOKEN = os.getenv('WIALON_TOKEN', 'default_token')
WIALON_HOST = os.getenv('WIALON_HOST', 'default_host')