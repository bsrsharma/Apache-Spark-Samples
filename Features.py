from __future__ import print_function

# replace a quoted string with commas with unquoted string with underscores
def removeEmbeddedCommas(str):

   quoteSeen = False
   LoC = [' ']*len(str)
   for i in range(len(str)):
      LoC[i] = str[i]
      if ((str[i] == "'" ) or (str[i] == '"')):
         if (quoteSeen == True):
            quoteSeen = False
         else:
            quoteSeen = True
            LoC[i] = ' '
      if ((str[i] == ',') and (quoteSeen == True)):
         LoC[i] = '_'
	 
   return (''.join(LoC))


# This function returns a 0.0 if the operand string can't be converted to a float; otherwise, the input is converted to float
def with0Str(str):
   try:
       f = float(str)
       return f
   except StandardError:
       return 0.0
     
import hashlib

def md5(str):
    m = hashlib.md5()
    m.update(str)
    return m.digest()
  
def removeStrings(str, k):
    LoS = str.split(',')
    Lof = [0.0]*k
    kk = 0
    for j in range(len(LoS)):
       try:
           f = float(LoS[j])
           Lof[kk] = f
           kk = kk + 1
       except StandardError:
           # no op
           kk = kk
          
    return Lof
  
from pyspark import SparkConf, SparkContext


conf = SparkConf().setAppName("Anonymizer").setMaster("local")
sc = SparkContext(conf=conf) 

def findFeatures(inputFileName, outputFileName):
  
  
   inpFile = sc.textFile(inputFileName)
   
   numRows = inpFile.count()

   print('\nRead ', numRows, ' rows from ', inputFileName, '\n')   

   print('Print out a few rows read from file')
  
   print('\n', inpFile.take(5), '\n' )
   
   # Rectangularize the RDD before vectorizing
  
   # Filter elements to remove quotes to prevent (quote) embedded commas
  
   countFields = inpFile.map(lambda s: removeEmbeddedCommas(s)).map(lambda s: len(s.split(','))).collect()
   
   print('number of fields in each row (first few): ', countFields[0:4])
   
   RectangularizationNeeded = False
   maxCount = 0
   maxCountAt = 0
   
   for i in range(len(countFields)):
      if (countFields[i] > maxCount):
         maxCount = countFields[i]
         maxCountAt = i
      if (i > 0) and (RectangularizationNeeded == False):
	 if (countFields[i] != countFields[i-1]):
	    RectangularizationNeeded = True
	    
   if (RectangularizationNeeded == True):
	 print('Identified jagged data set; Rectangularization needed')
   else:
	 print('Identified rectangular data set')
	    
   print('Inferring longest row(s) has ', maxCount, ' fields at row ', maxCountAt)
   
   inpFileRe = inpFile.map(lambda s: removeEmbeddedCommas(s)).map(lambda s: s + ',No Data')
   # remove short rows
   shortFile = inpFileRe.filter(lambda row: len(row.split(',')) < maxCount+1)
   print("Short rows will be filtered out")		
   print('\n', shortFile.take(10), '\n')
   # truncate to maxCount+1 columns
   inpFileTr = inpFileRe.filter(lambda row: len(row.split(',')) == maxCount + 1)
   print('\n', inpFileTr.take(5), '\n' )
  
   header = inpFileTr.first()
   hL = header.split(',')
   
   inpFileNh = inpFileTr.filter(lambda row: row != header)
   
   print('Removed the First row as Header')
   numRows = inpFileNh.count()
   print('number of rows = ', numRows) 
   
   from pyspark.mllib.linalg import Matrix, Matrices
   from pyspark.mllib.linalg import Vector, Vectors

   # parsedData will be org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vector]
   parsedData = inpFileNh.map(lambda s: Vectors.dense([with0Str(t) for t in s.split(',')]))
   print('\nprint out a few vectors after converting from strings\n')
   print( parsedData.take(5) )


   from pyspark.mllib.stat import MultivariateStatisticalSummary, Statistics

   summary = Statistics.colStats(parsedData)

   print('\nprint out summary statistics, for each column\n')
   
   print('summary.mean'); print(summary.mean())
   print('summary.variance'); print(summary.variance())
   print('summary.count'); print(summary.count())
   print('summary.max'); print(summary.max())
   print('summary.min'); print(summary.min())
   print('summary.normL1'); print(summary.normL1())
   print('summary.normL2'); print(summary.normL2())
   print('summary.numnonZeros'); print(summary.numNonzeros()); print() 
   
   numCols = len(summary.mean())
   
   
   typeStrings = [' ']*numCols

   # infer columns where normL1, normL2, mean, variance, max and mean are 0 as non-numeric

   print('Inferring column data types:')
   
   import math   
   
   for j in range(numCols):
      if ((summary.normL1()[j] == 0.0) and (summary.normL2()[j] == 0.0) and (summary.mean()[j] == 0.0) and (summary.variance()[j] == 0.0) and (summary.max()[j] == 0.0) and (summary.min()[j] == 0.0)):
         typeStrings[j] = 'String'
      else:
         if ((math.trunc(summary.normL1()[j]) == summary.normL1()[j]) and (math.trunc(summary.max()[j]) == summary.max()[j]) and (math.trunc(summary.min()[j]) == summary.min()[j]) ):
            typeStrings[j] = 'Int'
         else:
            typeStrings[j] = 'Float'
            
      print(typeStrings[j], end=',')              

   print('\n\n')
   
#******************************************************************************
# take out the 'String' columns before calling Statistics.corr()

   numNumericCols = 0
   for j in range(numCols):
     if (typeStrings[j] != 'String'):
        numNumericCols = numNumericCols + 1

   noStrings = inpFileNh.map(lambda s: Vectors.dense(removeStrings(s, numNumericCols)))
   print( noStrings.take(5) )
   
   correlMatrix = Statistics.corr(noStrings, method='pearson')
   
   print('Computing Correlation Matrix on all columns')
   print('Printing out column names that have correlation coefficient > 0.5 or < -0.5')

   for i in range(numNumericCols):
      for j in range(i):
         if (((correlMatrix[i][j] >= 0.5) or (correlMatrix[i][j] <= -0.5)) and (i != j)):
            print(hA[i], hA[j], correlMatrix[i][j])
			
#******************************************************************************
#******************************************************************************

# create a contingency matrix

 
   LoLoF = [[0.0 for x in range(numNumericCols)] for y in range(numRows)]
	    
   LoLoF = noStrings.collect()
   
   pdLinArr = [0.0 for x in range(numNumericCols*numRows)]
   
   for i in range(numRows):
      for j in range(numNumericCols):
         pdLinArr[i*numNumericCols + j] = abs(LoLoF[i][j])

   mat = Matrices.dense(numRows, numNumericCols, pdLinArr)

# conduct Pearson's independence test on the input contingency matrix
   print("Computing Pearson's independence test on the input contingency matrix using chi-square test")
   
   independenceTestResult = Statistics.chiSqTest(mat)
   
# summary of the test including the p-value, degrees of freedom
   print('%s\n' % independenceTestResult)


#*******************************************************************************

   stdDev = [0.0]*numCols
   
   for j in range (numCols):
      stdDev[j] = math.sqrt(summary.variance()[j])
      
#*******************************************************************************
#   test for normal distribution using Kolmogorov-Smirnov test
#
   colVec = [0.0]*numRows
   
   #vecRDD = sc.parallelize(colVec)
   #testResult = Statistics.kolmogorovSmirnovTest(vecRDD, 'norm', 0, 1)
   #print(testResult)
   
   numericMean = [0.0]*numNumericCols
   numericSD = [0.0]*numNumericCols

   k = 0   
   for j in range(numCols):
      if ((summary.mean()[j] != 0.0) and (summary.variance()[j] != 0.0)):
         numericMean[k] = summary.mean()[j]
         numericSD[k] = stdDev[j]
         k = k + 1
   
   print('Checking if column data is normally distributed using Kolmogorov-Smirnov test')   

   for j in range(numNumericCols):
      for i in range(numRows):
	 # see https://issues.apache.org/jira/browse/SPARK-20802
	 # test fails if data is normally distributed
	 # kolmogorovSmirnovTest in pyspark.mllib.stat.Statistics throws net.razorvine.pickle.PickleException
	 # when input data is normally distributed (no error when data is not normally distributed)
         colVec[i] = float(i) # LoLoF[i][j]
      vecRDD = sc.parallelize(colVec)
      print(colVec[0], colVec[numRows-1], numericMean[j], numericSD[j])
      testResult = Statistics.kolmogorovSmirnovTest(vecRDD, 'norm', numericMean[j], numericSD[j])
      print(testResult)
    
   
#*******************************************************************************
#*******************************************************************************
#
#   estimate kernel densities
#
   from pyspark.mllib.stat import KernelDensity

   # colVec = [0.0]*numRows
   # vecRDD = sc.parallelize(colVec)
   
   print('Computing kernel densities on all columns using a Bandwidth of 3.0')

   kd = KernelDensity()
   kd.setSample(vecRDD)
   kd.setBandwidth(3.0)
  
   sAS = int(math.sqrt(numRows))                                   # sample array size
   samplePoints = [0.0]*sAS
   #samplePoints = [0.0]*numRows

   for i in range(sAS):
      samplePoints[i] = float(i*sAS)
   #for i in range(numRows):
   #   samplePoints[i] = float(i)

   densities = kd.estimate(samplePoints)	 
   
   print('Estimating kernel densities')
   
   print('Print kernel densities at sample points')
   #print('Print kernel densities > 0.01 at sample points')
   for j in range(numNumericCols):
        # print( hL[j])
        for i in range(numRows):
	   # see https://issues.apache.org/jira/browse/SPARK-20803
	   # KernelDensity.estimate in pyspark.mllib.stat.KernelDensity throws
	   # net.razorvine.pickle.PickleException when input data is normally
	   # distributed (no error when data is not normally distributed)
           colVec[i] = float(i) # LoLoF[i][j]
    	vecRDD = sc.parallelize(colVec)
        kd = KernelDensity()
        kd.setSample(vecRDD)
        kd.setBandwidth(3.0)
        # Find density estimates for the given values
        densities = kd.estimate(samplePoints)
        for i in range(sAS):
           print(densities[i], end=',')
        print()
	#for i in range(numRows):
        #   if (densities[i] >= 0.01):
        #       print(i, densities[i], end=',')
        print()               

#*******************************************************************************

#*******************************************************************************
#
#  compute Skewness and Kurtosis for each numeric column
#
   skew = [0.0]*numNumericCols
   kurt = [0.0]*numNumericCols
   term = 0.0
   
   k = 0
   for j in range(numCols):
      if (typeStrings[j] != 'String'):
         skew[k] = 0.0
         kurt[k] = 0.0
         # extra work: find Ints
         typeStrings[j] = 'Int'
         meanj = summary.mean()[j]
         for i in range(numRows):
            if ((typeStrings[j] == 'Int') and (math.trunc(LoLoF[i][k]) != LoLoF[i][k])):
               typeStrings[j] = 'Float'
            term = (LoLoF[i][k] - meanj)/stdDev[j]
            skew[k] = skew[k] + (term*term*term)
            kurt[k] = kurt[k] + (term*term*term*term)
         skew[k] = skew[k]/numRows
         kurt[k] = (kurt[k]/numRows) - 3.0
         k = k + 1            

   print('Skewness of columns')
   k = 0
   for j in range(numCols):
      if (typeStrings[j] == 'String'):
         print('Text', end=',')
      else:
	 print(skew[k], end=',')
      	 k = k+1
   print()
   
   print('Kurtosis of columns')
   k = 0
   for j in range(numCols):
      if (typeStrings[j] == 'String'):
         print('Text', end=',')
      else:
	 print(kurt[k], end=',')
      	 k = k+1
   print()   
   
   print('Inferring column data types (Text string, Int, Float)')
   
   # numbers that are Int and non-negative and  "large" are likely to be numeric labels -- keep checking this heuristic
   # columns that are outside Kurtosis limits <-1.2, 3.0> may be numeric labels

   print('Attempting to infer if an Int column is a numeric label')
   print("If all Ints in a column are >= 0 and 'large', it may be numLabel")
   print('If all Ints in a column are >= 0 and excess kurtosis is outside [-1.2, 3.0], it may be numLabel')
   
   for j in range(numCols):
      if ((typeStrings[j] == 'Int') and (summary.min()[j] >= 0) and ((summary.max()[j] > 10000) or (kurt[j] < -1.2) or (kurt[j] > 3.0))):
         print('column ' + j + ' (' + hA[j] + ') ' + ' may be a numeric label')
         typeStrings[j] = 'NumLabel'

   
#******************************************************************************
#******************************************************************************
#
#   Normalize the dataset by shifting by mean and scaling by stdDev
#
   normData = [[0.0 for x in range(numNumericCols)] for y in range(numRows)]
   rowMaxs = [0.0]*numRows
   rowMins = [0.0]*numRows
   rowNormL1s = [0.0]*numRows
   rowNormL2s = [0.0]*numRows
   rowNumZeros = [0]*numRows
   means = [0.0]*numCols
   
   for j in range(numCols):
      means[j] = summary.mean()[j]

   for i in range(numRows):
      rowMaxs[i] = -999999.0
      rowMins[i] = 999999.0
      rowNumZeros[i] = 0
      rowNormL1s[i] = 0.0
      rowNormL2s[i] = 0.0
      
      k=0
      for j in range(numCols):
	 if ((typeStrings[j] == 'Int') or (typeStrings[j] == 'Float')):
            normData[i][k] = (LoLoF[i][k] - means[j])/stdDev[j]
	    if (normData[i][k] > rowMaxs[i]):
	       rowMaxs[i] = normData[i][k]
            if (normData[i][k] < rowMins[i]):
	       rowMins[i] = normData[i][k]
            if (normData[i][k] == 0.0):
	       rowNumZeros[i] = rowNumZeros
            if (abs(normData[i][k]) < 100.0):
	       rowNormL1s[i] = rowNormL1s[i] + abs(normData[i][k])
	       rowNormL2s[i] = rowNormL2s[i] + normData[i][k]*normData[i][k]
	    # print(i,j,k, LoLoF[i][k], means[j], stdDev[j], normData[i][k], rowNormL1s[i], rowNormL2s[i])
            k = k + 1	    

   input = open(inputFileName, 'r')
   fileHandle = open('/home/bsrsharma/work/python/rowNormL1L2.csv', 'w')
   
   # Keep upto 6 columns of identifying info
   if (numCols > 1):
      for j in range(min(5 , numCols)):
         fileHandle.write( hL[j]); fileHandle.write(',')
   fileHandle.write( 'L1-Norm' ); fileHandle.write(",")
   fileHandle.write( 'L2-Norm\n' )
   
   s = input.readline() # don't repeat header
   
   for i in range(numRows):
      # copy input to output
      s = input.readline()
      LoS  = s.split(',')     
      for j in range(min(5 , numCols)):
         fileHandle.write( LoS[j]); fileHandle.write(',')
      fileHandle.write('%s' % rowNormL1s[i]); fileHandle.write(',')
      fileHandle.write('%s' % math.sqrt(rowNormL2s[i])); fileHandle.write('\n')

   fileHandle.close()
   input.close()

   print('Wrote ', 'rowNormL1L2.csv')

   input = open(inputFileName, 'r')
   fileHandle = open(outputFileName, 'w')

   # output normalized data
   numCols = numCols - 1
   # write header row
   if (numCols > 1):
      for j in range(numCols-1):
         fileHandle.write( hL[j]); fileHandle.write(',')
   fileHandle.write( hL[numCols-1] ); fileHandle.write( '\n' )
   
   s = input.readline() # don't repeat header
   
   for i in range(numRows):
      # copy input to output
      s = input.readline()
      LoS  = s.split(',')
      k=0
      for j in range(numCols-1):
         if (typeStrings[j] == 'String'):
            fileHandle.write( LoS[j])
         else:
	    fileHandle.write( '%s' % normData[i][k])
	    k=k+1
	 fileHandle.write(',')
      if (typeStrings[numCols-1] == 'String'):
	 fileHandle.write( LoS[numCols-1])
      else:
	 fileHandle.write( '%s' % normData[i][k])
      fileHandle.write('\n')

   fileHandle.close()
   input.close()

   print('Wrote ', outputFileName, '\n')
   
#******************************************************************************
  
# compute median for each column

   medians = [0.0]*numNumericCols
   aCol = [0.0]*numRows

   for j in range(numNumericCols):
      for i in range(numRows):
	 aCol[i] = LoLoF[i][j]
      aCol.sort()

      medians[j] = aCol[numRows/2]

   print('medians:')
   k = 0
   for j in range(numCols):
      if (typeStrings[j] == 'String'):
         print('Text', end=',')
      else:         
         print(medians[k], end=',')
         k=k+1
   print('\n\n')      
   
# compute histograms for each column

   numBins = int(math.sqrt(numRows))
   histogram = [0]*(numBins+1)
   binWidth = 0
   mins = [0.0]*numCols
   maxs = [0.0]*numCols
      
   print('Computing histograms for numeric columns')
   print('choosing ', numBins, ' bins')
   
   k = 0
   
   for j in range(numCols):
      mins[j] = summary.min()[j]
      maxs[j] = summary.max()[j]
      if (typeStrings[j] == 'String'):
	 print('column ', j, '( ', hL[j], ' ): Text')
      else:	
         binWidth = (maxs[j] - mins[j])/numBins
         for i in range(numBins):
            histogram[i] = 0
         for i in range(numRows):
            histogram[int((LoLoF[i][k] - mins[j])/binWidth)] += 1
         print('column ', j, '( ', hL[j], ' ):')
         if (typeStrings[j] == 'NumLabel'):
            print('NumLabel')
         for i in range(numBins):
            print(histogram[i], end=',')
         print()           
         k = k + 1
   print('\n\n')
   
# compute modes

   modes = [0.0]*numNumericCols
   largestBin = 0
   binIndex = 0
   
   print('modes:')
   k = 0
   for j in range(numCols):
      if (typeStrings[j] == 'String'):
         print('Text', end=',') 	
      else:	
	 largestBin = 0
	 binIndex = 0
	 for i in range(numBins):
            # pick the bin with most items
            if (histogram[i] > largestBin):
                binIndex = i
         modes[k] = mins[j] + (maxs[j] - mins[j])*binIndex/numBins
         print(modes[k], end=',')
         k = k + 1
   print('\n\n')
   
   
   return 0
 
 
import sys
from random import random
from operator import add

from pyspark.sql import SparkSession
   

if __name__ == "__main__":
    """
        Usage: Features("/home/bsrsharma/work/scala/arran.csv", "/home/bsrsharma/work/python/features.csv")

    """
    spark = SparkSession.builder.appName("Features").getOrCreate()

    rc = -1
    rc = findFeatures("/home/bsrsharma/work/python/arran.csv", "/home/bsrsharma/work/python/features.csv"  )
    
    print("findFeatures ended with ", rc)
  
  
  
   
  