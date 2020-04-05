from time import sleep, clock
import random


def startCountdown():
    timeout = 3+3*random.random()
    print('timeout = ', timeout, '\n')
    start = clock()
    while True:
        elapsed_time = clock() - start
        if elapsed_time > timeout:
            print('Countodwn elapsed: ', elapsed_time)
            break
        else:
            print(elapsed_time, end = '\r')

startCountdown()


count = 20
for i in range(0, count):
    if i<count-1:
        print('Printing number ', i, end='\r')
    else:
        print('Printing number ', i)
    sleep(.05)