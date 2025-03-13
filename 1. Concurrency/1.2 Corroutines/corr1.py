def corr1(ss):
    print('Send me a string')
    try:
        while True:
            text = (yield)
            if ss in text:
                print(f'Found {ss}')
    except GeneratorExit:
        print('Quitting')


# Sample usage
# >>> g = corr1('hail satan')
# >>> next(g)
# Send me a string
# >>> g.send("tsonta")
# >>> g.send("heil")
# >>> g.send("heil satan heil")
# >>> g.send("hail satan hail")
# Found hail satan
# >>> g.close()
# Quitting