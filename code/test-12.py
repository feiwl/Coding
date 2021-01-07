
def app_csv(csv):
    with open(csv, 'r') as file:
        test = list(map(lambda line: (''.join(line.split(',')[1].split()),
                                      ''.join(line.split(',')[3].split())), file))
        result = dict()
        test.pop(0)
        for k, v in test:
            result.update({k: v})
        return result

def out_csv(csv):
    with open(csv, 'r') as file:
        test = list(map(lambda line:(line.split(',')[2].split('.')[0],
                                     ''.join(line.split(',')[3].split())), file))
        result = dict()
        for k, v in test:
            result.update({k: v})
        return result


path='/home/banruo/work/109292000175_Position2020-05-21.csv'

with open(path, 'r') as file:
    for i in file:
        print(i.split(','))







