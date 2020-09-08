import sys
import os

# app's path
sys.path.insert(0, os.path.abspath(os.path.curdir))

from direstplus import app  # NOQA

# Initialize WSGI app object
application = app
