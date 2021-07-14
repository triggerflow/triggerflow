import requests
from collections import defaultdict


def main(args):
    func_id = args['func_id']
    parallel = args['parallel']
    url = args['url']
    res = requests.head(url)
    size = int(res.headers['Content-Length'])
    chunk = size // parallel
    byte_range = func_id * chunk, (func_id * chunk) + chunk

    res = requests.get(url, headers={'Range': 'bytes={}-{}'.format(byte_range[0], byte_range[1])})
    text = res.text.split()

    words = defaultdict(int)
    for word in text:
        words[word] += 1

    word_list = [(word, count) for word, count in words.items()]
    print(word_list)
    word_list.sort(key=lambda elem: elem[1], reverse=True)
    result = {}
    for word, count in word_list[:50]:
        result[word] = count

    return result
