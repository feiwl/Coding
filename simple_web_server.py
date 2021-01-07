from aiohttp import web,log
import logging
import json

async def indexhandle(request:web.Request):
    return web.Response(text='welcome to pyserver', status=200)

async def handle(request:web.Request):
    print(request.match_info)
    print(request.query_string)
    return web.Response(text=request.match_info.get('id', '0000'), status=200)

async def todopost(request:web.Request):
    print(request.method)
    print(request.match_info)
    print(request.query_string)
    print(request.json())
    js = await request.json()
    print(js, type(js))
    text = dict(await request.post())
    print(text, type(text))
    js.update(text)
    res = json.dumps(js)
    print(res)

    return web.Response(text=res, status=201)

app = web.Application()
app.router.add_get('/', indexhandle)
app.router.add_get('/{id}', handle)
app.router.add_post('/api/todo', todopost)

app.logger.setLevel(level=logging.NOTSET)
web.run_app(app, host='0.0.0.0', port=8080)



