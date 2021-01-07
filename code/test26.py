import json
from collections import Counter
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

def hanle(appcsv, outcsv):
    result = dict()
    result_1=dict()
    result_2=dict()
    for k, v in outcsv.items():
        result[k[0:6]] = str(v)
    for k in appcsv.keys():
        if result.get(k) != appcsv.get(k) and appcsv.get(k) != '0':
            result_1.update({(k, appcsv.get(k)):(k, result.get(k))})
    for k in result.keys():
        if result.get(k) != appcsv.get(k) and appcsv.get(k) != '0':
            result_2.update({(k, appcsv.get(k)):(k, result.get(k))})
    return result_1,result_2

parser = argparse.ArgumentParser(description="Data compare")
parser.add_argument('--appcsv', required=True, help="Application Output Data")
parser.add_argument('--outcsv', required=True, help="Server Output Data")
args=parser.parse_args()

data=json.loads(open(args.outcsv).read())
parrot = data['parrot']['109292000175']
pigeon = data['pigeon']['109292000175']
result = Counter()
result.update(Counter(parrot))
result.update(Counter(pigeon))
result_data = dict(result)

print("\x1b[31m{}\x1b[m".format(hanle(app_csv(args.appcsv),result_data)))



