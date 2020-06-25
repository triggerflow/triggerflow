from collections import defaultdict


def main(args):
    dicts = args['dicts']
    merged = defaultdict(int)

    for words in dicts:
        for k, v in words.items():
            merged[k] += v

    return merged
