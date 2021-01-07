
import threading
import asyncio

async def sleep(x):
    for i in range(3):
        print('sleep {}'.format(i))
        await asyncio.sleep(x)

async def showthread(x):
    for i in range(3):
        print(threading.enumerate())
        await asyncio.sleep(2)

loop = asyncio.get_event_loop()
tasks = [sleep(3), showthread(3)]
loop.run_until_complete(asyncio.wait(tasks))
loop.close()
