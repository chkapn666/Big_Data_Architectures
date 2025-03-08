def div(x, y):
    if y == 0:
        raise Exception('Divisor is zero.')
    return x / y


x = 10
y = 2

try:
    z = div(x, y)
    print(f'Result is {z}')
except Exception as ex:
    print(ex)
else:
    print('Successful completion')
finally:
    print('Clean up')
