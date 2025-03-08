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
# >>> g = corr1('hail hitler')
# >>> next(g)
# Send me a string
# >>> g.send("tsonta")
# >>> g.send("heil")
# >>> g.send("heil hitler heil")
# >>> g.send("hail hitler hail")
# Found hail hitler
# >>> g.close()
# Quitting