import sys

max_value = 21 + 1

for i in range(1, max_value, 2):
    result = 0
    plus_minus = -1
    for index in range(1, i+1, 2):
        plus_minus = plus_minus * (-1)
        result = result + index * plus_minus
    print('To : ', i, ', result : ', result)


girl_group = {"jerry": 1, "john": 2, "jenny": 3}
for k, v in girl_group.items():
    print(k + " : ", str(v))


