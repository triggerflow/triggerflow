import numpy as np


def main(kwargs):
    loop_count = kwargs.get('loops', 50)

    value = 0
    for _ in range(loop_count):
        x = np.random.rand()  # Generate random point between 0 and 1
        y = np.random.rand()
        z = np.sqrt((x ** 2) + (y ** 2))
        if z <= 1:  # Point is inside circle
            value += 1

    est_pi = float(value) * 4 / loop_count
    return {'pi': est_pi}
