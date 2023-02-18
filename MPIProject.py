#Compile Status: Compiling
#Program Status: Working


from mpi4py import MPI
import sys

#variables
comm = MPI.COMM_WORLD
world_size = comm.Get_size()
rank = comm.Get_rank()
input_file = sys.argv[2]
test_file = sys.argv[6]
merge_method = sys.argv[4]
inputfile = open(input_file, "r")
listOfLines = inputfile.readlines()
numberOfLines = len(listOfLines)
numberOfWorkers = world_size-1

#since master node has rank 0 it executes the code inside
if rank == 0:
    # if input data can be distributed evenly
    if numberOfLines%numberOfWorkers==0:
        numberOfLinesPerWorker = numberOfLines//numberOfWorkers
        for i in range(world_size-1):
            comm.send(listOfLines[numberOfLinesPerWorker*i:numberOfLinesPerWorker*(i+1)],dest=i+1)
    # if it cannot be distribute evenly, it is distributed as even as possible
    else:
        count = 0
        numberOfLinesPerWorker = numberOfLines//numberOfWorkers + 1
        for i in range(world_size-1):
            if numberOfLines % (numberOfWorkers-i) != 0 :
                comm.send(listOfLines[count:count+numberOfLinesPerWorker],dest=i+1)
                numberOfLines -= numberOfLinesPerWorker
                count += numberOfLinesPerWorker
            else:
                comm.send(listOfLines[count:count + numberOfLinesPerWorker - 1],dest=i+1)
                numberOfLines -= numberOfLinesPerWorker-1
                count += (numberOfLinesPerWorker -1)
    ngramDictionary = {}
    # master collects all the dictionaries from workers and puts them into a new dictionary
    if merge_method == "MASTER":
        for i in range(1,world_size):
            data = comm.recv(source=i)
            for key in data.keys():
                if key in ngramDictionary.keys():
                    ngramDictionary[key] += data[key]
                else:
                    ngramDictionary[key] = data[key]
    # master recieves final dictionary from the worker with the highest rank
    else:
        ngramDictionary = comm.recv(source=numberOfWorkers)
    
    #reading "test.txt"
    testFile = open(test_file,"r")
    testFileLiles = testFile.readlines()
    #calculating probabilities and printing them into the console
    for line in testFileLiles:
        lineSplitted = line.split()
        firstKey = lineSplitted[0] + " " + lineSplitted[1]
        secondKey = lineSplitted[0]
        print("Probability: {}  Bigram: {}".format(ngramDictionary[firstKey]/ngramDictionary[secondKey],firstKey)) 
# workers execute the code inside this else statement
else:
   dictionary = {}
   data = comm.recv(source=0)
   # printing out how much line each worker received
   print("The worker with rank {} received {} sentences".format(rank,len(data)))
   # for each line bigrams and unigrams are counted and dictionary is updated
   for line in data:
        splittedData = line.split()
        splittedData = splittedData[1:len(splittedData)-1]
        for i in range(len(splittedData)):
            if splittedData[i] not in dictionary.keys():
                dictionary[splittedData[i]] = 1
            else:
                dictionary[splittedData[i]] += 1

            if i != len(splittedData)-1:
                biagram = splittedData[i] + " " + splittedData[i+1]
                if biagram not in dictionary.keys():
                    dictionary[biagram] = 1
                else:
                    dictionary[biagram] += 1
    # each worker sends it dictionary to the master node
   if merge_method == "MASTER":
        comm.send(dictionary,dest=0)
    # each worker merges its dictionary with the received dictionary from the worker with rank (rank-1) and sends result dictionary to the worker with rank (rank + 1)
   elif merge_method == "WORKERS":
        # first worker only sends its dicitonary
        if rank == 1:
            if numberOfWorkers != 1:
                comm.send(dictionary,dest = 2)
            else:
                comm.send(dictionary,dest = 0)
        elif rank == numberOfWorkers:
            data = comm.recv(source= rank-1)
            for key in data.keys():
                if key in dictionary.keys():
                    dictionary[key] += data[key]
                else:
                    dictionary[key] = data[key]
            comm.send(dictionary,dest = 0)
        # last worker sends result dictionary to the master node  
        else:
            data = comm.recv(source= (rank-1))
            for key in data.keys():
                if key in dictionary.keys():
                    dictionary[key] += data[key]
                else:
                    dictionary[key] = data[key]
            comm.send(dictionary,dest = (rank+1))
   

 