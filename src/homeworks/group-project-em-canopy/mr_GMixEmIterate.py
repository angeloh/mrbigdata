'''
Created on Apr 18, 2011

'''
from mrjob.job import MRJob

from math import sqrt, exp, pow,pi
from numpy import zeros, shape, random, array, zeros_like, dot, linalg, add
import json
import os


def dist(x,y):
    #euclidean distance between two lists    
    sum = 0.0
    for i in range(len(x)):
        temp = x[i] - y[i]
        sum += temp * temp
    return sqrt(sum)

def gauss(x, mu, P_1):
    xtemp = x - mu
    n = len(x)
    p = exp(- 0.5*dot(xtemp,dot(P_1,xtemp)))
    detP = 1/linalg.det(P_1)
    p = p/(pow(2.0*pi,n/2.0)*sqrt(detP))
    return p

class MrGMixEm(MRJob):
    DEFAULT_PROTOCOL = 'json'
    
    def __init__(self, *args, **kwargs):
        super(MrGMixEm, self).__init__(*args, **kwargs)
        
        fullPath = self.options.pathName + 'intermediateResults.txt'
        fileIn = open(fullPath)
        inputJson = fileIn.read()
        fileIn.close()
        inputList = json.loads(inputJson)
        temp = inputList[0]        
        self.phi = array(temp)           #prior class probabilities
        temp = inputList[1]
        self.means = array(temp)         #current means list
        temp = inputList[2]
        self.cov_1 = array(temp)         #inverse covariance matrices for w, calc.
        #accumulate partial sums                               
        #sum of weights - by cluster
        self.new_phi = zeros_like(self.phi)        #partial weighted sum of weights
        self.new_means = zeros_like(self.means)
        self.new_cov = zeros_like(self.cov_1)
        
        self.numMappers = 1             #number of mappers
        self.count = 0                  #passes through mapper
        
        #import Canopy list
        canopyListPath= self.options.pathName + 'canopylist.txt'
        fileIn = open(canopyListPath)
        inputJson = fileIn.read()
        fileIn.close()
        self.canopyList = json.loads(inputJson)
        
        self.membership=[]            #assign means to canopy
        
#        print self.canopyList[1]
#        print self.means
#        jDebug = json.dumps([self.canopyList,self.means])    
#        debugPath = self.options.pathName + 'debug2.txt'
#        fileOut = open(debugPath,'w')
#        fileOut.write(jDebug)
#        fileOut.close()
        
        
        for can in self.canopyList:
            ismember=zeros(self.options.k)
            i=0
            for meanval in self.means:
                #print can
                #print meanval
                if dist(array(can),meanval)<self.options.t1:
                    ismember[i]=1
                i=i+1
            #print ismember
            self.membership.append(ismember)
        #print self.membership
                  
        
                                                 
    def configure_options(self):
        super(MrGMixEm, self).configure_options()

        self.add_passthrough_option(
            '--k', dest='k', default=4, type='int',
            help='k: number of densities in mixture')
        self.add_passthrough_option(
            '--t1', dest='t1', default=10.0, type='float',
            help='t1: out circle distance')
        self.add_passthrough_option(
            '--pathName', dest='pathName', default=os.getcwd()+'/data/', type='str',
            help='pathName: pathname where intermediateResults.txt is stored')
        
    def mapper(self, key, val):
        #accumulate partial sums for each mapper
        xList = json.loads(val)
        x = array(xList)
        
        samecanopy= zeros(self.options.k)
        i=0
        for can in self.canopyList:
            if dist(array(can),x)<self.options.t1:
                samecanopy=add(samecanopy,self.membership[i])     
            i=i+1
        
        
        wtVect = zeros_like(self.phi)
        for i in range(self.options.k):
            wtVect[i]=0.000001
            if samecanopy[i]>0: wtVect[i] = self.phi[i]*gauss(x,self.means[i],self.cov_1[i])
                
        wtSum = sum(wtVect)
        wtVect = wtVect/wtSum
        #accumulate to update est of probability densities.
        #increment count
        self.count += 1
        #accumulate weights for phi est
        self.new_phi = self.new_phi + wtVect
        for i in range(self.options.k):
            if samecanopy[i]>0:
                #accumulate weighted x's for mean calc
                self.new_means[i] = self.new_means[i] + wtVect[i]*x
                #accumulate weighted squares for cov estimate
                xmm = x - self.means[i]
                covInc = zeros_like(self.new_cov[i])
            
                for l in range(len(xmm)):
                    for m in range(len(xmm)):
                        covInc[l][m] = xmm[l]*xmm[m]
                self.new_cov[i] = self.new_cov[i] + wtVect[i]*covInc
                    
        
        #dummy yield - real output passes to mapper_final in self
        if False: yield 1,2
        
    def mapper_final(self):
        
        out = [self.count, (self.new_phi).tolist(), (self.new_means).tolist(), (self.new_cov).tolist()]
        jOut = json.dumps(out)        
        
        yield 1,jOut
    
    
    def reducer(self, key, xs):
        #accumulate partial sums
        first = True        
        #accumulate partial sums
        for val in xs:
            if first:
                temp = json.loads(val)
                
               
                
                totCount = temp[0]
                totPhi = array(temp[1])
                totMeans = array(temp[2])
                totCov = array(temp[3])                
                first = False
            else:
                temp = json.loads(val)
                totCount = totCount + temp[0]
                totPhi = totPhi + array(temp[1])
                totMeans = totMeans + array(temp[2])
                totCov = totCov + array(temp[3])
        #finish calculation of new probability parameters
        newPhi = totPhi/totCount
        #initialize these to something handy to get the right size arrays
        newMeans = totMeans
        newCov_1 = totCov
        for i in range(self.options.k):
            newMeans[i,:] = totMeans[i,:]/totPhi[i]
            tempCov = totCov[i,:,:]/totPhi[i]
            #almost done.  just need to invert the cov matrix.  invert here to save doing a matrix inversion
            #with every input data point.
            newCov_1[i,:,:] = linalg.inv(tempCov)
        
        outputList = [newPhi.tolist(), newMeans.tolist(), newCov_1.tolist()]
        jsonOut = json.dumps(outputList)
        
        #write new parameters to file
        fullPath = self.options.pathName + 'intermediateResults.txt'
        fileOut = open(fullPath,'w')
        fileOut.write(jsonOut)
        fileOut.close()
        if False: yield 1,2

if __name__ == '__main__':
    MrGMixEm.run()