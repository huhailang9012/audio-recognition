# -*- coding: UTF-8 -*-
import json

class Person(object):

    def __init__(self, age, sex):
        self.age = age
        self.sex = sex


if __name__ == '__main__':
    p = Person(30, 'female')
    print(p.age)
    print(p.sex)
    s = json.dumps(p.__dict__)
    print(s)
# json_string = json.dumps([ob.__dict__ for ob in list_obj])