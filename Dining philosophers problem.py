from mpi4py import MPI
import random
import time

TAG_REQUEST = 1
TAG_SEND = 2

leftNeighbour = None
rightNeighbour = None
leftFork = None
rightFork = None 
needToSendLeftFork = False
needToSendRightFork = False
wasLeftFirst = True 

class Fork:
    def __init__(self):
        self.dirty = True

    def soil(self):
        self.dirty = True

    def clean(self):
        self.dirty = False
    
    def isDirty(self):
        return self.dirty
    
def generate_tabs(num_tabs):
    return '\t' * num_tabs
       

comm = MPI.COMM_WORLD
rank = comm.Get_rank() 
size = comm.Get_size()
n = size
   
if (rank == 0):
    leftNeighbour = n - 1
    rightNeighbour = rank + 1
    leftFork = Fork()
    rightFork = Fork()
if (rank == n - 1):
    leftNeighbour = rank - 1
    rightNeighbour = 0
    leftFork = None
    rightFork = None
if (rank > 0 and rank < n - 1):
    leftNeighbour = rank - 1
    rightNeighbour = rank + 1
    rightFork = Fork()
    leftFork = None
needToSendLeftFork = False
needToSendRightFork = False
wasLeftFirst = True


def respondRequests():
    global leftNeighbour, rightNeighbour, leftFork, rightFork, rightFork, needToSendLeftFork, needToSendRightFork, wasLeftFirst, comm
    status = MPI.Status()
    flag = comm.iprobe(source=leftNeighbour, tag=TAG_REQUEST, status = status)
    if(flag):
        if (status.Get_source() == leftNeighbour and status.Get_tag() == TAG_REQUEST):
            if(leftFork):
                if(leftFork.isDirty()):
                    leftFork.clean()
                    comm.send(None, dest = leftNeighbour, tag = TAG_SEND)
                    leftFork = None
                    print(generate_tabs(rank), "Philosopher", rank, "sent left fork\n", flush= True)
            else:
                needToSendLeftFork = True
                if(needToSendRightFork):
                    wasLeftFirst = False
                else:
                    wasLeftFirst = True
    
    flag = comm.iprobe(source=rightNeighbour, tag=TAG_REQUEST, status = status)
    if(flag):
        if (status.Get_source() == rightNeighbour and status.Get_tag() == TAG_REQUEST):
            if(rightFork):
                if(rightFork.isDirty()):
                    rightFork.clean()
                    comm.send(None, dest = rightNeighbour, tag = TAG_SEND)
                    rightFork = None
                    print(generate_tabs(rank), "Philosopher", rank, "sent right fork\n", flush= True)
            else:
                needToSendRightFork = True
                if(needToSendLeftFork):
                    wasLeftFirst = True
                else:
                    wasLeftFirst = False

def think():
    think_time = random.randint(2, 5)
    end_time = time.time() + think_time
    while(time.time() < end_time):
        print(generate_tabs(rank), "Philosopher ", rank, " is thinking\n", flush= True)
        respondRequests()
        time.sleep(1)

def eat():
    eat_time = random.randint(5, 10)
    print(generate_tabs(rank), "Philosopher ", rank, "is eating\n", flush= True)
    time.sleep(eat_time)


def philosopher():
        global leftNeighbour, rightNeighbour, leftFork, rightFork, rightFork, needToSendLeftFork, needToSendRightFork, wasLeftFirst
        status = MPI.Status()
        while(True):
            think()
            while (leftFork == None or rightFork == None):
                if(leftFork == None):
                    comm.send(None, dest=leftNeighbour, tag=TAG_REQUEST)
                    print(generate_tabs(rank), "Philosopher " ,rank, " sends request for left fork\n", flush= True)
                while(leftFork == None):
                    respondRequests()
                    flag = comm.iprobe(source=leftNeighbour, tag=TAG_SEND, status = status)
                    if(flag and status.Get_source() == leftNeighbour and status.Get_tag() == TAG_SEND):
                        comm.recv(None, source=leftNeighbour, tag=TAG_SEND)
                        leftFork = Fork()
                        leftFork.clean()
                        print(generate_tabs(rank), "Philosopher ", rank, " received left fork\n", flush= True)

                if(rightFork == None):
                    comm.send(None, dest=rightNeighbour, tag=TAG_REQUEST)
                    print(generate_tabs(rank), "Philosopher " ,rank, " sends request for right fork\n", flush= True)
                while(rightFork == None):
                    respondRequests()
                    flag = comm.iprobe(source=rightNeighbour, tag=TAG_SEND, status = status)
                    if(flag and status.Get_source() == rightNeighbour and status.Get_tag() == TAG_SEND):
                        comm.recv(None, source=rightNeighbour, tag=TAG_SEND)
                        rightFork = Fork()
                        rightFork.clean()
                        print(generate_tabs(rank), "Philosopher ", rank, " received right fork\n", flush= True)
            eat()
            leftFork.soil()
            rightFork.soil()

            if(wasLeftFirst):
                if(needToSendLeftFork):
                    comm.recv(None, source=leftNeighbour, tag=TAG_REQUEST)
                    leftFork.clean()
                    comm.send(None, dest=leftNeighbour, tag=TAG_SEND)
                    leftFork = None
                    print(generate_tabs(rank), "Philosopher ", rank, " sent left fork\n", flush= True)
                if(needToSendRightFork):
                    comm.recv(None, source=rightNeighbour, tag=TAG_REQUEST)
                    rightFork.clean()
                    comm.send(None, dest=rightNeighbour, tag=TAG_SEND)
                    rightFork = None 
                    print(generate_tabs(rank), "Philosopher ", rank, " sent right fork\n", flush= True)
            else:
                if(needToSendRightFork):
                    comm.recv(None, source=rightNeighbour, tag=TAG_REQUEST)
                    rightFork.clean()
                    comm.send(None, dest=rightNeighbour, tag=TAG_SEND)
                    rightFork = None
                    print(generate_tabs(rank), "Philosopher ", rank, " sent right fork\n", flush= True)
                if(needToSendLeftFork):
                    comm.recv(None, source=leftNeighbour, tag=TAG_REQUEST)
                    leftFork.clean()
                    comm.send(None, dest=leftNeighbour, tag=TAG_SEND)
                    leftFork = None 
                    print(generate_tabs(rank), "Philosopher ", rank, " sent left fork\n", flush= True)
    
            needToSendLeftFork = False
            needToSendRightFork = False
            wasLeftFirst = True

philosopher()
MPI.Finalize()