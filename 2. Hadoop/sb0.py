from snakebite.client import Client

client = Client('localhost', 9000)
for f in client.ls(['/']):
    print(f)

for _ in client.mkdir(['/testdir']):
    pass

for f in client.ls(['/']):
    print(f)
