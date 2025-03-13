from my_app1 import ts

# We do not know when we'll receive the results of distributed tasks => we need to collect them asynchronously. 
# Invoke the ".delay()" method.
res = ts.delay('ACG ITC6107')

# Then we spin loop (?) until we've got results.
while not res.ready():
    pass

# And then we access the ".result" 

print(f'Result: {res.result}')

