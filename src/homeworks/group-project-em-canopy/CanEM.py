'''
Created on May 18, 2011

@author: CanEM Team
'''
from mr_CanopyIterate import MrCanopy
#from mr_AssignCentToCan import MrAssignCentToCan
from mr_GMixEmInitialize import MrGMixEmInit
from mr_GMixEmIterate import MrGMixEm
import json
from math import sqrt
import os 

def dist(x,y):
    #euclidean distance between two lists    
    sum = 0.0
    for i in range(len(x)):
        temp = x[i] - y[i]
        sum += temp * temp
    return sqrt(sum)


'''
Canopy EM for gaussian mixture model.  
sequence of events
1.  Generate Canopy with parameter t2   (mr_CanopyIterate.py)
2.  initialize with modified kmeans initializer (mr_GmixEmInitialize.py)
2.  generate 1/0 initial weight vector based on cluster membership (mr_GmixEmInitialize.py)
3.  run through calc to generate first set of phi, mu, sigma (probably sigma inverse) (mr_GmixEmInitialize.py)
4.  iteration - if a data entry is in the same canopy with a cluster's mean (determined by parameter t1), 
               then 
                    mapper employs phi, mu, sigma calculated in reducer to calc weights for input examples
                    and generates partial sums for phi, mu, sigma inverse calc.  
               otherwise  
                    mapper directly assigns a very very small value as the weight and ignore this point when calculating 
                    partial sums for phi, mu, sigma inverse calc

'''

def main():
    
    #data path parameters
    filePath = os.getcwd() + "/data/"
    inputDataName="input.txt"        #the dataset you want to rung clustering
    intermediateDataName="intermediateResults.txt"    #intermediate file for EM
    canopyList="canopylist.txt"          # list of canopy centers
    #canopyCentroidAssign="canopyCentroidAssign.txt"  
    
    print 'Canopy-EM cluster by CanEM Team'   
    
    #Generate Canopies
    print 'Generating Canopies...'   
    #canopyforEM=[] 
    mrJob0 = MrCanopy(args=[filePath+inputDataName])
    with mrJob0.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                key, value = mrJob0.parse_output_line(line) #only one key; so only one line
                #canopyforEM.append(value)
    
    #write canopies to file
    canOut = json.dumps(value)
    fileOut = open(filePath+canopyList,'w')
    fileOut.write(canOut)
    fileOut.close()


    #Run the EM initializer to get starting centroids
    print 'Initializing...'

    mrJob = MrGMixEmInit(args=[filePath+inputDataName])
    with mrJob.make_runner() as runner:
        runner.run()
    
    #pull out the centroid values to compare with values after one iteration
    fileIn = open(filePath+intermediateDataName)
    paramJson = fileIn.read()
    fileIn.close()
    
    delta = 10
    #Begin iteration on change in centroids
    print 'Iterating...'
    while delta > 0.01:
        
        
#        #assign centroid to canopy
#        mrJob3 = MrAssignCentToCan(args=[filePath+intermediateDataName])
#        with mrJob3.make_runner() as runner:
#            runner.run()
#        
        
        
        
        #parse old centroid values
        oldParam = json.loads(paramJson)
        #run one iteration
        oldMeans = oldParam[1]
        mrJob2 = MrGMixEm(args=[filePath+inputDataName])
        with mrJob2.make_runner() as runner:
            runner.run()
            
        #compare new centroids to old ones
        fileIn = open(filePath+intermediateDataName)
        paramJson = fileIn.read()
        fileIn.close()
        newParam = json.loads(paramJson)
        
        k_means = len(newParam[1])
        newMeans = newParam[1]
        
        delta = 0.0
        for i in range(k_means):
            delta += dist(newMeans[i],oldMeans[i])
        
        print delta

if __name__ == '__main__':
    main()