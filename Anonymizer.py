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
     
# This function takes a string and generates a similar looking anonymized string

def makeAnonStr(str): 
   LoC = [' ']*len(str)
   md5LoB = [' ']*16
   substrEnd = 0
   myInt = 0
   c = ' '
   
   for i in range(len(str)):
      if (i % 16 == 0):
         substrEnd = i + 16
         if (i + 16 > len(str)):
            substrEnd =  len(str)
         md5LoB = md5(str)
   
      c = str[i]
	  
      LoC[i] = c
      myInt = ord(md5LoB[i % 16])
      
      if (c >= 'A' and c <= 'Z'):
	    LoC[i] = chr((myInt % 26) + ord('A'))
		
      if (c >= 'a' and c <= 'z'):
	    LoC[i] = chr((myInt % 26) + ord('a'))

      if (c >= '0' and c <= '9'):
	    LoC[i] = chr((myInt % 10) + ord('0'))
		
   return (''.join(LoC))

from pyspark import SparkConf, SparkContext


conf = SparkConf().setAppName("Anonymizer").setMaster("local")
sc = SparkContext(conf=conf) 

def makeSubstData(inputFileName, outputFileName):
  
  
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
   
   stdDev = [0.0]*numCols
   
   import math
   
   for j in range (numCols):
      stdDev[j] = math.sqrt(summary.variance()[j])
   
   typeStrings = [' ']*numCols

   # infer columns where normL1, normL2, mean, variance, max and mean are 0 as non-numeric

   print('Inferring column data types:')
   
   for j in range(numCols):
      if ((summary.normL1()[j] == 0.0) and (summary.normL2()[j] == 0.0) and (summary.mean()[j] == 0.0) and (summary.variance()[j] == 0.0) and (summary.max()[j] == 0.0) and (summary.min()[j] == 0.0)):
         typeStrings[j] = 'String'
      else:
         if ((math.trunc(summary.normL1()[j]) == summary.normL1()[j]) and (math.trunc(summary.max()[j]) == summary.max()[j]) and (math.trunc(summary.min()[j]) == summary.min()[j]) ):
            typeStrings[j] = 'Int'
         else:
            typeStrings[j] = 'Double'
            
      print(typeStrings[j], end=',')              

   print('\n\n')
  
  
   LoLoStr = inpFileNh.map(lambda s: s.split(',')).collect()
   
   for i in range(min(4,numRows)):
      for j in range(numCols):
         print( LoLoStr[i][j], end=',' )
      print()
   print()      
   
  
   arrn = [[0.0 for x in range(numCols)] for y in range(numRows)]
   vID = [' ']*30
   
   import random
   
   mean = [0.0]*numCols
   
   for j in range (numCols):
     mean[j] = summary.mean()[j]
 
   for j in range (numCols):
      for i in range (numRows):
	 arrn[i][j] = random.gauss(mean[j], stdDev[j])
 
   print()
   for i in range (min(4, numRows)):
      for j in range (max(1, numCols - 1)):
	if (typeStrings[j] == 'String'):
           print(makeAnonStr(LoLoStr[i][j]), end=',')
        else:
           print(arrn[i][j], end=',')
      if (typeStrings[numCols-1] == 'String'):
         print(makeAnonStr(LoLoStr[i][numCols-1]))
      else:	
         print(arrn[i][numCols-1])
   print()
   
   fileHandle = open(outputFileName, 'w')

   # put back header
   if (numCols > 2):
         for j in range (max(1 , numCols - 2)):
	    fileHandle.write(hL[j] + ',')
   fileHandle.write(hL[numCols-2] + '\n')
         
   for i in range(numRows):
      if (numCols > 2):
         for j in range (max(1 , numCols - 2)):
	    if (typeStrings[j] == 'String'):
	       fileHandle.write(makeAnonStr(LoLoStr[i][j])+ ',')
	    else:   
               fileHandle.write('%s' % arrn[i][j])
               fileHandle.write(',')
      if (typeStrings[numCols-2] == 'String'):
         fileHandle.write(makeAnonStr(LoLoStr[i][numCols-2]))
      else:	    
         fileHandle.write('%s' % arrn[i][numCols-2])
      fileHandle.write('\n')
   fileHandle.close()

   print('Wrote',  outputFileName, '\n')
   
   outpFile = sc.textFile(outputFileName)
   
   numRows = outpFile.count()

   print('\nRead ', numRows, ' rows from ', outputFileName, '\n')

   print('Print out a few rows read from file')
  
   print('\n', outpFile.take(5), '\n' )
   
   parsedOutData = outpFile.map(lambda s: Vectors.dense([with0Str(t) for t in s.split(',')]))
   
   summary = Statistics.colStats(parsedOutData)

   print('\nprint out summary statistics, for each column\n')
   
   print('summary.mean'); print(summary.mean())
   print('summary.variance'); print(summary.variance())
   print('summary.count'); print(summary.count())
   print('summary.max'); print(summary.max())
   print('summary.min'); print(summary.min())
   print('summary.normL1'); print(summary.normL1())
   print('summary.normL2'); print(summary.normL2())
   print('summary.numnonZeros'); print(summary.numNonzeros()); print()   
  
   return 0

import sys
from random import random
from operator import add

from pyspark.sql import SparkSession
   

if __name__ == "__main__":
    """
        Usage: Anonymizer("/home/bsrsharma/work/scala/arran.csv", "/home/bsrsharma/work/python/subst.csv")

    """
    spark = SparkSession.builder.appName("Anonymizer").getOrCreate()

    rc = -1
    rc = makeSubstData("/home/bsrsharma/work/python/arran.csv", "/home/bsrsharma/work/python/subst.csv"  )
    
    print("makeSubstData ended with ", rc)
  
  
  

