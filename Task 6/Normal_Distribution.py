from pyspark.sql import SparkSession
from pyspark.mllib.random import RandomRDDs
from pyspark.mllib.linalg import Matrices, Vectors
from pyspark.mllib.stat import Statistics
import math

DEVIATION = 1.3
MEAN = 5
SIZE = 1000

def generate_normal_distribution():
    global SIZE
    
    partitions = []    
    for i in range ( MEAN * 2 + 1 ):
        partitions.append(i)
    
    n_rdd = RandomRDDs.normalRDD(sc, SIZE)    
    score = n_rdd.map(lambda x: MEAN + DEVIATION * x)
    
    h = score.histogram (partitions)
    histogram = h[1]       
    
    SIZE = 0
    for x in histogram:
        SIZE += x
        
    observed = []
    for x in histogram:
        observed.append( x / SIZE )
        
    print (observed)
        
    vec_o = Vectors.dense(observed)
    
    return vec_o
        
def calculate_theoretical_frequencies():
    expected = []    
    for i in range ( MEAN * 2 ):
        x = i + 0.5
        t = ( x - MEAN ) / DEVIATION
        fi = math.exp ( - math.pow (t,2) / 2) / math.sqrt(2 * math.pi)
        n = round (SIZE * fi / DEVIATION / 1000,3)
        expected.append (n)
    
    print (expected)
    
    vec_e = Vectors.dense(expected)
    
    return vec_e
    
def hypothesis_test():
    vec_o = generate_normal_distribution()
    vec_e = calculate_theoretical_frequencies()
    goodnessOfFitTestResult = Statistics.chiSqTest(vec_o,vec_e)
    print("%s\n" % goodnessOfFitTestResult)


sc = SparkContext.getOrCreate()    
hypothesis_test()
sc.stop()