'''
Created on Mar 18, 2011

@author: mike-bowles
'''


from numpy import random
import json
import os

#pathname="//home//mike-bowles//pyWorkspace//mapReducers//src//mr_kMeans2//"

pathname = os.getcwd() + "/data/"
#pathname="C:\\Users\\zhenyuyan\\Documents\\Hadoop\\pythonworkspace\\kMeans\\"
filename="input.txt"
fileOut=open(pathname+filename,"w") 
#generate a 2-dim example.  5 centers picked randomly in (0,10) each with 
#100 samples of gaussian unit variance samples


#centers = []
#ncenters = 5
#for i in range(ncenters):
#    x = 10*random.uniform()
#    y = 10*random.uniform()
#    centers.append([x,y])
#    
##centers = []
##ncenters = 2
##centers.append([0.0,0.0])
##centers.append([2.0,2.0])
#print centers
#for i in range(100):
#    for j in range(ncenters):
#        xm = centers[j][0]
#        ym = centers[j][1]
#        x = random.normal(xm,1.0,1)[0]
#        y = random.normal(ym,1.0,1)[0]
#        outString = json.dumps([x,y]) + "\n"
#        fileOut.write(outString)
#        
#fileOut.close()


centers = []
ncenters = 4
ndim=3
npoints=50

for i in range(ncenters):
    c=[0.0]*ndim 
    for j in range(ndim):
        c[j] = 10*random.uniform()
        #x.append[temp]
    centers.append(c)

#centers = []
#ncenters = 2
#centers.append([0.0,0.0])
#centers.append([3.0,3.0])


print centers
for i in range(npoints):
    for j in range(ncenters):
        x=[0.0]*ndim 
        for k in range(ndim):
            x[k]=random.normal(centers[j][k],1.0,1)[0]
        outString = json.dumps(x) + "\n"
        fileOut.write(outString)
        
fileOut.close()



