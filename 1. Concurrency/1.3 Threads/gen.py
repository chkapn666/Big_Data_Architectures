"""
An iterator is an object that has a state - it knows where it is 
right now and where it will go on the next iteration.
"""

nums = [1, 2, 3]
print('__iter__' in dir(nums))