from random import uniform


def gen_data(it, size=10000):
    with open('account' + str(it).zfill(2) + '.txt', 'w') as f:
        it = 10000 * it
        for i in range(size):
            tar = 'insert into account values(' + str(12500000 + it + i) + ', \"name' + str(it + i) + '\", ' + str(
                round(uniform(1, 1000), 3)) + ');\n'
            f.write(tar)
        f.close()


if __name__ == '__main__':
    for i in range(10):
        gen_data(i)
