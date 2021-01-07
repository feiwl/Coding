
class Dispatcher:
    def __init__(self):
        self.cmds = {}

    def reg(self, cmd, fn):
        if isinstance(cmd, str):
            self.cmds[cmd] = fn
        else:
            print('error')

    def run(self):
        while True:
            cmd = input("plz input command: ")
            if cmd.strip() == "quit":
                return
            self.cmds.get(cmd.strip(), self.defaultfn)()

    def defaultfn(self):
        pass

    def __call__(self, *args, **kwargs):
        return self.reg, self.run

# reg, run = Dispatcher()()
#
# reg('cmd1', lambda : 1)
# reg('cmd2', lambda : 2)
#
# run()

class disaptcher:

    def cmd1(self):
        print('cmd1')

    def reg(self, cmd, fn):
        if isinstance(cmd, str):
            setattr(self.__class__, cmd, fn)
        else:
            print('error')

    def run(self):
        while True:
            cmd = input("plz input command: ")
            if cmd.strip() == 'quit':
                return
            getattr(self, cmd.strip(), self.defaultfn)()

    def defaultfn(self):
        print('default')

dis = disaptcher()

dis.reg('cmd2', lambda self: print(2))
dis.reg('cmd3', lambda self: print(3))
dis.run()
