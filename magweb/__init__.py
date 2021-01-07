# 200行
from .web import MagWeb
import json

def jsonify(**kwargs):
    content = json.dumps(kwargs)
    response = MagWeb.Response()
    response.content_type = "application/json"
    response.body = "{}".format(content).encode()
    return response
