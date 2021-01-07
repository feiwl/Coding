from magweb import MagWeb, jsonify
from wsgiref.simple_server import make_server

# 创建Router
idx = MagWeb.Router()
py = MagWeb.Router('/python')

# 一定要注册
MagWeb.register(idx)
MagWeb.register(py)

@idx.get('^/$|^/index')  # 只匹配根
def index(ctx, request: MagWeb.Request) -> MagWeb.Response:  # 增加ctx
    res = MagWeb.Response()
    res.status_code = 200
    res.content_type = 'text/html'
    res.charset = 'utf-8'
    res.body = '<h1>马哥教育欢迎你. magedu.com</h1>'.encode()
    return res

@py.get('/{id:int}')  # 只匹配get方法/python/xxx
def showpython(ctx, request: MagWeb.Request) -> MagWeb.Response:  # 增加ctx
    print("~~~~~~~~~~~~~~~~~" + ctx.router.prefix)
    res = MagWeb.Response()
    # res.content_type = 'text/plain'
    res.content_type = 'text/html'
    res.body = '<h1>Welcome to Magedu Python. vars = {}</h1>'.format(request.vars.id).encode()
    return res

# 拦截器举例
@MagWeb.register_preinterceptor
def showheaders(ctx, request: MagWeb.Request) -> MagWeb.Request:
    print(request.path)
    print(request.user_agent)
    return request

@py.register_preinterceptor
def showprefix(ctx, request: MagWeb.Request) -> MagWeb.Request:
    print('~~~~~prefix = {}'.format(ctx.router.prefix))
    return request

if __name__ == "__main__":
    ip = '192.168.10.101'
    port = 9999
    server = make_server(ip, port, MagWeb())

    try:
        server.serve_forever()  # server.handle_request() 一次
    except KeyboardInterrupt:
        server.shutdown()
        server.server_close()