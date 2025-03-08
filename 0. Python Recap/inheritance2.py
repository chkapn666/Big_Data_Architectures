class Animal:
    def say(self):
        return '---'

class Dog(Animal):
    # I define a method with a name matching that of the parent class' method => the behavior is overwritten for the child objects
    # This is one of the typical reasons for which we resort to the definition of parent-child class structures; we want to alter some
    # aspects of the behavior of the upper-level class(es)
    def say(self):
        return 'Woof'

class Cat(Animal):
    def say(self):
        return 'Miaw'

class Sheep(Animal):
    def say(self):
        return 'Bee'

for a in [Sheep(), Dog(), Cat()]:
    print(a.say())