import argparse

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

def hanle(appcsv, outcsv):
    result_1=dict()
    result_2=dict()
    for k in appcsv.keys():
        if outcsv.get(k) != appcsv.get(k) and appcsv.get(k) != '0':
            result_1.update({(k, appcsv.get(k)):(k, outcsv.get(k))})

    for k in outcsv.keys():
        if outcsv.get(k) != appcsv.get(k) and appcsv.get(k) != '0':
            result_2.update({(k, appcsv.get(k)):(k, outcsv.get(k))})

    return result_1,result_2

parser = argparse.ArgumentParser(description="Data compare")
parser.add_argument('--appcsv', required=True, help="Application Output Data")
parser.add_argument('--outcsv', required=True, help="Server Output Data")
args=parser.parse_args()
# path='/home/banruo/work/109292000175_Position2020-05-20.csv'
# path1='/home/banruo/work/20200520.csv'

appcsv=app_csv(args.appcsv)
outcsv=out_csv(args.outcsv)

print(hanle(appcsv,outcsv))





