'''
Created on Apr 18, 2011


'''
from mrjob.job import MRJob

from math import sqrt  #, exp, pow,pi
from numpy import zeros, shape, random, array, zeros_like, dot, linalg
import json
import os

def dist(x,y):
    #euclidean distance between two lists    
    sum = 0.0
    for i in range(len(x)):
        temp = x[i] - y[i]
        sum += temp * temp
    return sqrt(sum)


#def gauss(x, mu, P_1):
#    xtemp = x - mu
#    n = len(x)
#    p = exp(- 0.5*dot(xtemp,dot(P_1,xtemp)))
#    detP = 1/linalg.det(P_1)
#    p = p/(pow(2.0*pi,n/2.0)*sqrt(detP))
#    return p

class MrCanopy(MRJob):
    DEFAULT_PROTOCOL = 'json'
    
    def __init__(self, *args, **kwargs):
        super(MrCanopy, self).__init__(*args, **kwargs)
#        
        self.canopyCenters =[]
                                                 
    def configure_options(self):
        super(MrCanopy, self).configure_options()

        self.add_passthrough_option(
            '--k', dest='k', default=4, type='int',
            help='k: number of densities in mixture')
        self.add_passthrough_option(
            '--t2', dest='t2', default=3.5, type='float',
            help='t2: inner circle distance')
        self.add_passthrough_option(
            '--pathName', dest='pathName', default=os.getcwd()+'/data/', type='str',
            help='pathName: pathname where intermediateResults.txt is stored')
        
    def mapper(self, key, val):
        #accumulate partial sums for each mapper
        

        
        x = json.loads(val)
        
        if len(self.canopyCenters)==0:
            self.canopyCenters.append(x)
            yield 1,x
        else:
            iscenter=True
            for item in self.canopyCenters:
                if dist(array(x),item) <=self.options.t2*0.8:  #use a value smaller than t2
                    iscenter=False
                    break
            if iscenter==True:
                self.canopyCenters.append(x)
                yield 1,x    
        
#    def mapper_final(self):
#        
#        out = [self.count, (self.new_phi).tolist(), (self.new_means).tolist(), (self.new_cov).tolist()]
#        jOut = json.dumps(out)        
#        
#        yield 1,jOut
    
    
    def reducer(self, key, xs):
        
        canopyCentersReducer=[]
        
        for x in xs:
            if len(canopyCentersReducer)==0:
                canopyCentersReducer.append(x)
                #yield 1,x
            else:
                iscenter=True
                for item in canopyCentersReducer:
                    if dist(array(x),item) <=self.options.t2:  #use real t2
                        iscenter=False
                if iscenter==True:
                    canopyCentersReducer.append(x)
                #yield 1,x
        yield 1, canopyCentersReducer
        

if __name__ == '__main__':
    MrCanopy.run()