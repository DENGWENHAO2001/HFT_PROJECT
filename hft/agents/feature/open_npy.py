import numpy as np
data = np.load("second_feature.npy")

for i in range(len(data)):
    str_value = str(data[i])
    if '5' in str_value:
        new_value = str_value.replace('5', '2')
        data[i]=new_value
    if '10' in str_value:
        new_value = str_value.replace('10', '3')
        data[i]=new_value
    if '30' in str_value:
        new_value = str_value.replace('30', '4')
        data[i]=new_value
    if '60' in str_value:
        new_value = str_value.replace('60', '5')
        data[i]=new_value

print(data)
np.save('test_second_feature.npy', data)
# 5-1,10-2,30-3,60-4