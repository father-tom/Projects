# create parent class School
class School:
    def __init__(self, name, level, numberOfStudents):
        self.name = name
        self.level = level
        self.numberOfStudents = numberOfStudents
    
    # define getters
    def getName(self):
        return self.name
    
    def getLevel(self):
        return self.level
    
    def getNumOfStudents(self):
        return self.numberOfStudents
    
    # setter for number of students
    def setNumOfStudents(self, new_amount):
        self.numberOfStudents = new_amount
        
    def __repr__(self):
        return f'A {self.level} school named {self.name} with {self.numberOfStudents} students.'
    
# create Primary School class
class PrimarySchool(School):
    def __init__(self, name, numberOfStudents, pickUpPolicy):
        super().__init__(name, 'Primary', numberOfStudents)
        self.pickUpPolicy = pickUpPolicy
        
    def getPickUp(self):
        return self.pickUpPolicy
    
    def __repr__(self):
        parentRepr = super().__repr__()
        return parentRepr + f' The pickup polciy is {self.pickUpPolicy}.'


# create High School class
class HighSchool(School):
    def __init__(self, name, numberOfStudents, sportsTeams):
        super().__init__(name, 'High', numberOfStudents)
        self.sportsTeams = sportsTeams
        
    def getSportsTeams(self):
        return self.sportsTeams
    
    def __repr__(self):
        parentRepr = super().__repr__()
        return parentRepr + f' Information of our sport Team {", ".join(self.sportsTeams)}.'
    
# testing School class
a = School('Codecademy', 'high', 100)
print(a)
print(a.getName())
print(a.getLevel())
print(a.getNumOfStudents())
a.setNumOfStudents(200)
print(a.getNumOfStudents())

# testing Primary School class
b = PrimarySchool('Codecademy', 300, "Pickup Allowed")
print(b.getPickUp())
print(b)

# testing High School class
c = HighSchool('Codecademy High', 500, ['Tennis', 'Basketball'])
print(c.getSportsTeams())
print(c)