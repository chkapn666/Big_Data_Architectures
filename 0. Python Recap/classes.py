class Person:
    def __init__(self, fname, lname, height, weight):
        self.fname = fname
        self.lname = lname
        self.height = height
        self.weight = weight

    def set_height(self, height):
        self.height = height

    def set_weight(self, weight):
        self.weight = weight

    def __str__(self):
        return f'[Person: {self.fname}, {self.lname} (height: {self.height}, weight: {self.weight}]'


p1 = Person('John', 'Smith', 1.80, 92)
print(p1)

p1.set_weight(89)
print(p1)