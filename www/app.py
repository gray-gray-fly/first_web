import logging; logging.basicConfig(level=logging.INFO)

import asyncio, os, json, time
from datetime import datetime

from aiohttp import web,web_runner

routes = web.RouteTableDef()

@routes.get('/')
def index(request):
    return web.Response(body=b'<h1>Why are you so beautiful?</h1>', content_type='text/html')



def init():
    app = web.Application()
    app.add_routes([web.get('/',index)])
    logging.info('server started at http://127.0.0.1:8000...')
    web.run_app(app,host='127.0.0.1',port=8000)
    
init()