from aiohttp import web,log
import logging
import json

async def indexhandle(request:web.Request):
    return web.Response(text='welcome to pyserver', status=200)

async def handle(request:web.Request):
    print(request.match_info)
    print(request.query_string) # http://127.0.0.1:8080/1?name=12301
    return web.Response(text=request.match_info.get('id', '0000'), status=200)

async def todopost(request:web.Request):
    print(request.method)
    print(request.match_info)
    print(request.query_string) # http://127.0.0.1:8080/1?name=12301

    js = await request.json() # get post json data
    print(js, type(js))

    text = dict(await request.post())
    print(text, type(text))

    js.update(text)
    res = json.dumps(js)
    print(res)

    return web.Response(text=res, status=201)

app = web.Application()
app.router.add_get("/", indexhandle) # http://127.0.0.1:8080/
app.router.add_get("/{id}", handle) # http://127.0.0.1:8080/12301
app.router.add_post('/api/todo', todopost)

app.logger.setLevel(level=logging.NOTSET)
web.run_app(app, host='0.0.0.0', port=8080)