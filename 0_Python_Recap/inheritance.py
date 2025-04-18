# Python code to demonstrate how parent constructors
# are called.

# parent class
class Person(object):

    # __init__ is known as the constructor
    def __init__(self, name, idnumber):
        self.name = name
        self.idnumber = idnumber

    def display(self):
        print(self.name)
        print(self.idnumber)


# child class
class Employee(Person):
    def __init__(self, name, idnumber, salary, post):
        self.salary = salary
        self.post = post

        # invoking the __init__ of the parent class
        Person.__init__(self, name, idnumber)  # so the linked-parent Person object's values for 'name' and 'idnumber' are the same


# creation of an object variable or an instance
a = Employee('George', 312971, 60000, "Manager")

# calling a function of the class Person using its instance
a.display()
