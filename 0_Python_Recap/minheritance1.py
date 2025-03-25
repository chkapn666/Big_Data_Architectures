class C1:
    pass

class C2(C1):
    pass;

class C3(C1):
    pass

class C4(C2, C3):
    pass

print(C4.mro())
