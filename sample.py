import datetime
import time
import json
sample_dict = {'name':'omkar',
               'age':'23',
               'height':'179.23',
               'date':'8/27/2020',
               'time':'40:29.0',
               'time1':'23:19.2',
               'value':'12.42950111',
               'date1':'8/28/2020 14:22'}


def convert_data(value):
    try:
        value = int(value)
        return value
    except ValueError:
        pass
    try:
        value = float(value)
        return value
    except ValueError:
        pass
    

new_dict = {key:convert_data(value) for key,value in sample_dict.items()}

print(sample_dict)
print(new_dict)

for value in new_dict.values():
    print(type(value))