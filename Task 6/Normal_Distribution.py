from pyspark.sql import SparkSession
from pyspark.mllib.random import RandomRDDs
from pyspark.mllib.linalg import Matrices, Vectors
from pyspark.mllib.stat import Statistics
import math

# initial params
DEVIATION = 1.3
MEAN = 5
SIZE = 1000

def generate_normal_distribution():
    global SIZE
    
    # define partitions
    partitions = []    
    for i in range ( MEAN * 2 + 1 ):
        partitions.append(i)
        
    # generate normal distribution
    n_rdd = RandomRDDs.normalRDD(sc, SIZE)    
    score = n_rdd.map(lambda x: MEAN + DEVIATION * x)
    
    # get histogram
    h = score.histogram (partitions)
    histogram = h[1]       
    
    # calculate samle size inside defined partitions
    SIZE = 0
    for x in histogram:
        SIZE += x
        
    # calculate observed frequencies
    observed = []
    for x in histogram:
        observed.append( x / SIZE )
        
    print (observed)
        
    vec_o = Vectors.dense(observed)
    
    return vec_o
        
def calculate_theoretical_frequencies():
    
    # calculate theoretical frequencies
    expected = []    
    for i in range ( MEAN * 2 ):
        x = i + 0.5
        t = ( x - MEAN ) / DEVIATION
        fi = math.exp ( - math.pow (t,2) / 2) / math.sqrt(2 * math.pi)
        n = round ( fi / DEVIATION, 3)
        expected.append (n)
    
    print (expected)
    
    vec_e = Vectors.dense(expected)
    
    return vec_e
    
def hypothesis_test():
    vec_o = generate_normal_distribution()
    vec_e = calculate_theoretical_frequencies()
    
    # test hypothesis
    goodnessOfFitTestResult = Statistics.chiSqTest(vec_o,vec_e)
    print("%s\n" % goodnessOfFitTestResult)


sc = SparkContext.getOrCreate()    
hypothesis_test()
sc.stop()
