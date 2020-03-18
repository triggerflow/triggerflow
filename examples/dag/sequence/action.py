import time


def main(args):
    t0 = time.time()
    time.sleep(3)
    t1 = time.time()
    return {'start_time': t0, 'end_time': t1}
