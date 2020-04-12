from ConsensusModule import *
from time import sleep
import os

replicas = []
replicas.append(ConsensusModule('0',5))
replicas.append(ConsensusModule('1',5))
replicas.append(ConsensusModule('2',5))

for cm in replicas:
    cm.simulation_print()

for x in range(0,3):
    os.system(f"gnome-terminal -e 'bash -c \"tail -f ~/repos/Rock-em-Sock-em-RAFT/files/status{str(x)}.txt; exec bash\"'")

while True:
    for cm in replicas:
        cm.simulation_print()
    sleep(.3)