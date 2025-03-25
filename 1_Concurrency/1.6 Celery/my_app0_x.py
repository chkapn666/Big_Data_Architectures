# The workers will use the application in the my_app0 module for receiving tasks to execute.
from my_app0 import add
from my_app0 import sub
from my_app0 import mul
from my_app0 import div

r = add(10, 20)
print(f'Result: {str(r)}')

r = sub(10, 20)
print(f'Result: {str(r)}')

r = mul(10, 20)
print(f'Result: {str(r)}')

r = div(10, 20)
print(f'Result: {str(r)}')