import org.apache.log4j.{Level, LogManager, PropertyConfigurator}

/*******************************************************************************
//
// This will not run on yarn
//

class StringImprovements(val s: String) {
//
// This function returns a "0" if the operand string can't be converted to a number; otherwise, the input is unaltered
//
import scala.util.control.Exception._
   def toNumeric : String =
   {
      if ((allCatch.opt(s.toDouble)).isDefined == true)
	     return s
      else
         return ("0")
    }

// replace a quoted string with commas with unquoted string with underscores
  def unQuote : String =
  {	
     var quoteSeen  = false
	 var t = new Array[Char](s.length)
	 
     for (i <- 0 to s.length - 1)
	 {
	    t(i) = s.charAt(i)
        if ((s.charAt(i) == ''' ) || (s.charAt(i) == '"'))
	    {
	       if (quoteSeen == true)
		   {
              quoteSeen = false
           }
           else
           {
	          quoteSeen = true
           }			
           t(i) = ' '
        }
        if ((s.charAt(i) == ',') && (quoteSeen == true))
        {
	       t(i) = '_'
        }         	  
     }
	 
	 return t.mkString("")
  }  // unQuote
  
}  // StringImprovements  

*******************************************************************************/

object unQuote  {
  def removeEmbeddedCommas(s: String) : String =
  {	
     var quoteSeen  = false
	 var t = new Array[Char](s.length)
	 
     for (i <- 0 to s.length - 1)
	 {
	    t(i) = s.charAt(i)
        if ((s.charAt(i) == ''' ) || (s.charAt(i) == '"'))
	    {
	       if (quoteSeen == true)
		   {
              quoteSeen = false
           }
           else
           {
	          quoteSeen = true
           }			
           t(i) = ' '
        }
        if ((s.charAt(i) == ',') && (quoteSeen == true))
        {
	       t(i) = '_'
        }         	  
     }
	 
	 return t.mkString("")
  }  // removeEmbeddedCommas
  
}  // unQuote

object replaceText  {
import scala.util.control.Exception._
   def with0Str(s : String) : String = {
      if ((allCatch.opt(s.toDouble)).isDefined == true)
	     return s
      else
         return ("0")
    }
}  // replaceText			  
object feature {

   val MainLog = LogManager.getRootLogger
   val FeatureLog = LogManager.getLogger("FeatureLogger")
   
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

   val conf = new SparkConf().setAppName("Anonymizer")
   val sc = new SparkContext(conf)  	   

   def findFeatures(inputFileName : String, outputFileName : String, IntervalOption : String, UserSelectedLowerLimit : Double, UserSelectedUpperLimit : Double, FeatureOption : String) : Int =
   { 
   // Read from the input file an array of alfanumeric labels and numbers, compute the statistics of numbers and then
   // Check for inliers/outliers
   // IntervalOption : "Inlier" or "Outlier"
   // Feature options: "Mean", "Median", "Mode"

import org.apache.spark.mllib.linalg._

   // read inputFile to a vector
   // inputFile will be RDD[String]

   var inpFile = sc.textFile(inputFileName)

   var numRows = inpFile.count().toInt

   FeatureLog.info("Read " + inputFileName + ", Read " + numRows + " Rows")

   FeatureLog.trace("print out a few rows read from file")
   inpFile.take(5).foreach(FeatureLog.trace)
   
   
////////////////////////////////////////////////////////////////////////////////
// Rectangularize the RDD, if needed before vectorizing
//

//implicit def stringToString(s: String) = new StringImprovements(s)

// filter element to remove quotes to prevent (quote) embedded commas
  val inpFileUq = inpFile.map(s => unQuote.removeEmbeddedCommas(s) )
//   inpFileUq.take(5).foreach(FeatureLog.trace)			 

  val countFields = inpFileUq.map(s => s.split(',').length).collect()
  var RectangularizationNeeded = false
  
  FeatureLog.info("number of fields in each row: ")

  var maxCount = 0
  var maxCountAt = 0
  
  var outStr : String = "[" 
  
  for (i <- 0 to numRows - 1)
  {
    if (countFields(i) > maxCount)
	{
	   maxCount = countFields(i); maxCountAt = i
    }

	outStr = outStr + countFields(i).toString + ","
    if ((RectangularizationNeeded == false) && (i > 0))
	{
	   if (countFields(i) != countFields(i-1))
	   {
	      RectangularizationNeeded = true
	   }
    }	   
  }	

  outStr += "]" ; FeatureLog.trace(outStr)   
  
  if (RectangularizationNeeded == true)
	 FeatureLog.info("Identified jagged data set; Rectangularization needed")
  else
	 FeatureLog.info("Identified rectangular data set; No rectangularization needed")

// infer number of columns by picking the largest

  FeatureLog.info("Inferring longest row(s) has " + maxCount + " fields at row " + maxCountAt)
  
// rectangularize the RDD to numRows X maxCount

//  if (RectangularizationNeeded == true)
//  {
        var inpFileRe = inpFileUq.map(s => s + ",No Data")
//      inpFileRe.take(5).foreach(FeatureLog.trace)
// remove short rows
        val shortFile = inpFileRe.filter(row => row.split(',').length < maxCount+1)
        FeatureLog.trace("Short rows will be filtered out")		
        shortFile.take(40).foreach(FeatureLog.trace)	  
        val inpFileTr = inpFileRe.filter(row => row.split(',').length == maxCount+1)
//      inpFile.take(5).foreach(FeatureLog.trace)
//  }		
	
   // remove a header row
/*******************************************************************************   
   //var header = inpFile.first()                                       // header is a String
   //var hANum = header.split(',').map(_.toNumeric).map(_.toDouble)     // hANum is an Array of Double
   var header = new String
   var hANum = new Array[Double](maxCount+1)
   var allFieldsNonNumeric = true
   var allFieldsNonNumeric2 = true
   var hA = new Array[String](maxCount+1)
   var row2 = new String
   
   header = inpFile.first()
// FeatureLog.trace(header)   
   hANum = header.split(',').map(_.toNumeric).map(_.toDouble)   
   for (i <- 0 to maxCount - 1)
      if(hANum(i) != 0.0) allFieldsNonNumeric = false

   FeatureLog.trace("allFieldsNonNumeric = " + allFieldsNonNumeric)
   
   if (allFieldsNonNumeric == true)
   {
      inpFile = inpFile.filter(row => row != header)
//    check if row2 is a header
      row2 = inpFile.first()
//    FeatureLog.trace(row2)
      hANum = row2.split(',').map(_.toNumeric).map(_.toDouble)   
      for (i <- 0 to maxCount - 1)
         if(hANum(i) != 0.0) allFieldsNonNumeric2 = false

      FeatureLog.trace("allFieldsNonNumeric2 = " + allFieldsNonNumeric2)
      if (allFieldsNonNumeric2 == true)
         header = row2   
   }	  
*******************************************************************************/

   var header = inpFileTr.first()              // header is a String
   
   var hA = header.split(',')                 // hA is an Array of Strings
	  
   var inpFileNh = inpFileTr.filter(row => row != header)
   
   // split header into array of column header strings
   
//   if ((allFieldsNonNumeric == true) || (allFieldsNonNumeric2 == true))
//      hA = header.split(',')
	  
//   if (allFieldsNonNumeric2 == true) 
//      inpFile = inpFile.filter(row => (row != header) && (row != row2))

   FeatureLog.trace("Removed the Header row(s)")
   numRows = inpFileNh.count().toInt   
   FeatureLog.trace("number of rows = " + numRows)   

////////////////////////////////////////////////////////////////////////////////   
/*******************************************************************************
  if (RectangularizationNeeded == true)
  {
     val countFields2 = inpFile.map(s => s.split(',').length).collect()

     outStr = "[" 
     for (i <- 0 to numRows - 1)
     {
	   outStr = outStr + countFields2(i).toString + ","
     }	
     outStr += "]" ; FeatureLog.trace(outStr)
  }	 
*******************************************************************************/
import org.apache.spark.mllib.linalg.{Vector, Vectors}

  // parsedData will be org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vector]

   //val parsedData = inpFile.map(s => Vectors.dense(s.split(',').map(_.toNumeric).map(_.toDouble)))
   
   val parsedData = inpFileNh.map(s => Vectors.dense(s.split(',').map(replaceText.with0Str(_)).map(_.toDouble)))   

   FeatureLog.trace("print out a few vectors after converting from strings")
   parsedData.take(5).foreach(FeatureLog.trace)   

import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

   val summary: MultivariateStatisticalSummary = Statistics.colStats(parsedData)

   FeatureLog.info("print out summary statistics for each column")
   FeatureLog.info("summary.mean");  FeatureLog.info(summary.mean)
   FeatureLog.info("summary.variance");  FeatureLog.info(summary.variance)
   FeatureLog.info("summary.count");  FeatureLog.info(summary.count)
   FeatureLog.info("summary.max");  FeatureLog.info(summary.max)
   FeatureLog.info("summary.min");  FeatureLog.info(summary.min)
   FeatureLog.info("summary.normL1");  FeatureLog.info(summary.normL1)
   FeatureLog.info("summary.normL2");  FeatureLog.info(summary.normL2)
   FeatureLog.info("summary.numNonzeros");  FeatureLog.info(summary.numNonzeros)
   
   var numCols = summary.mean.size
   
/******************************************************************************/ 

   val correlMatrix: Matrix = Statistics.corr(parsedData, "pearson")
   
   FeatureLog.info("Computing Correlation Matrix on all columns")
   FeatureLog.info("Printing out column names that have correlation coefficient > 0.5 or < -0.5")

   for (i <- 0 to numCols - 1)
      for (j <- 0 to i)
	     if (((correlMatrix(i, j) >= 0.5) || (correlMatrix(i, j) <= -0.5)) && (i != j))
		    FeatureLog.info(hA(i), hA(j), correlMatrix(i, j))
			
/******************************************************************************/
   
import Array._

   var means = summary.mean.toArray
   var variances = summary.variance.toArray
   var normL1s = summary.normL1.toArray
   var normL2s = summary.normL2.toArray
   var maxs = summary.max.toArray
   var mins = summary.min.toArray
   var numNonzeros = summary.numNonzeros.toArray

   var typeStrings = new Array[String](numCols)

   // infer columns where normL1, normL2, mean, variance, max and mean are 0 as non-numeric

   
   for (j <- 0 to numCols -1)
   {
      if ((normL1s(j) == 0) && (normL2s(j) == 0) && (means(j) == 0) && (variances(j) == 0) && (maxs(j) == 0) && (mins(j) == 0))
      {
         typeStrings(j) = "String"
      }	  
      else
      {
         typeStrings(j) = "Double"		 
      }
   }
   

// compute Standard Deviation (sigma)   

   var stdDev = new Array[Double](numCols)
   
   outStr = "[" 
   for (j <- 0 to numCols - 1)
   {
	  if (j != 0) outStr += ","
      if (typeStrings(j) == "String")
      {
         stdDev(j) = 0
	     outStr = outStr + " Text "		 
      }
      else
      {	  
         stdDev(j) = Math.sqrt(summary.variance(j))
	     outStr = outStr + stdDev(j).toString		 
      }		 
	  
   }
   outStr += "]" ; FeatureLog.info("stdDevs");  FeatureLog.info(outStr)   
   
   val pdArr = parsedData.collect() // pdArr will be a 2-D Array of Double
   
/******************************************************************************/
/*******************************************************************************
//
//   Normalize the dataset by shifting by mean and scaling by stdDev
//
   var normData = ofDim[Double](numRows, numCols)
   var rowMaxs , rowMins, rowNormL1s, rowNormL2s = new Array[Double](numRows)
   var rowNumZeros = new Array[Int](numRows)


   for (i <- 0 to numRows -1)
   {
     rowMaxs(i) = -999999.0
  	 rowMins(i) = 999999.0
	   rowNumZeros(i) = 0
	   rowNormL1s(i) = 0.0
	   rowNormL2s(i) = 0.0
	 
     for ( j <- 0 to numCols - 1 )
     {
       normData(i)(j) = (pdArr(i)(j) - means(j))/stdDev(j)
		   if (normData(i)(j) > rowMaxs(i)) rowMaxs(i) = normData(i)(j)
		   if (normData(i)(j) < rowMins(i)) rowMins(i) = normData(i)(j)
		   if (normData(i)(j) == 0.0) rowNumZeros(i) += 1
       if (math.abs(normData(i)(j)) < 100.0)
		   {
		     rowNormL1s(i) += math.abs(normData(i)(j))
		     rowNormL2s(i) += normData(i)(j)*normData(i)(j)
       }			
     }
   }
   
// output a .csv


import java.io.PrintWriter

   val fileHandle = new PrintWriter(outputFileName)

   // Add Header
   if (numCols > 1)
   {
      for (j <- 0 to math.max(0 , numCols - 2))
      {
          fileHandle.print( hA(j)); fileHandle.print(",")	  
	    }
   }
   fileHandle.println( hA(numCols - 1))   


   val parsedStringData = inpFileNh.flatMap(s => s.split(','))   // parsedStringData is an RDD
   val pSA = parsedStringData.collect()                        // pSA is an Array of String   

   for (i <- 0 to numRows - 1)
   {
     if (numCols > 1)
     {
        for (j <- 0 to math.max(0 , numCols - 2))
        {
           if (typeStrings(j) == "String")
  	       {
		          // copy input to output
              fileHandle.print( pSA(numCols*i + j)); fileHandle.print(",")
           }
           else
           {
              if (typeStrings(j) == "Int")
		          {
	              fileHandle.print(String.valueOf(normData(i)(j).toInt))
                fileHandle.print(",")
              }
              else
              {		   
	             fileHandle.print(String.valueOf(normData(i)(j)))
                 fileHandle.print(",")
              }			  
		      }
        }
     }	 

     if (typeStrings(numCols - 1) == "String")
     {
	    // copy input to output
		  fileHandle.println( pSA(numCols*i + numCols - 1))
     }
     else
     {
       if (typeStrings(numCols - 1) == "Int")
       {	 
	      fileHandle.println(String.valueOf(normData(i)(numCols - 1).toInt))
       }
       else
	   {
          fileHandle.println(String.valueOf(normData(i)(numCols - 1)))
     }
     }		
   }

   fileHandle.close()

   FeatureLog.info("Wrote " + outputFileName)
   

// print end and middle values

   var sortedRowMaxs = new Array[Tuple2[Int, Double]](numRows)
   var sortedRowMins = new Array[Tuple2[Int, Double]](numRows)
   var sortedRowNumZeros = new Array[Tuple2[Int, Int]](numRows)   
   var sortedRowNormL1s = new Array[Tuple2[Int, Double]](numRows)
   var sortedRowNormL2s = new Array[Tuple2[Int, Double]](numRows)   

   for (i <- 0 to numRows - 1)
   {
      sortedRowMaxs(i) = (i, rowMaxs(i))
      sortedRowMins(i) = (i, rowMins(i))
      sortedRowNumZeros(i) = (i, rowNumZeros(i))
      sortedRowNormL1s(i) = (i, rowNormL1s(i))
      sortedRowNormL2s(i) = (i, rowNormL2s(i))	  
	  
   }

   scala.util.Sorting.stableSort(sortedRowMaxs, (e1: (Int, Double), e2: (Int, Double)) => e1._2 < e2._2)
   scala.util.Sorting.stableSort(sortedRowMins, (e1: (Int, Double), e2: (Int, Double)) => e1._2 < e2._2)
   scala.util.Sorting.stableSort(sortedRowNumZeros, (e1: (Int, Int), e2: (Int, Int)) => e1._2 < e2._2)
   scala.util.Sorting.stableSort(sortedRowNormL1s, (e1: (Int, Double), e2: (Int, Double)) => e1._2 < e2._2)
   scala.util.Sorting.stableSort(sortedRowNormL2s, (e1: (Int, Double), e2: (Int, Double)) => e1._2 < e2._2)
   
   FeatureLog.info("Normalized row maxes at rows, values:")
   FeatureLog.info(sortedRowMaxs(0), sortedRowMaxs(1), sortedRowMaxs(2), sortedRowMaxs(3), sortedRowMaxs(4) )
   FeatureLog.info(sortedRowMaxs(numRows-1), sortedRowMaxs(numRows-2), sortedRowMaxs(numRows-3), sortedRowMaxs(numRows-4), sortedRowMaxs(numRows-5) )
   
   FeatureLog.info("Normalized row mins at rows, values:")
   FeatureLog.info(sortedRowMins(0), sortedRowMins(1), sortedRowMins(2), sortedRowMins(3), sortedRowMins(4) )
   FeatureLog.info(sortedRowMins(numRows-1), sortedRowMins(numRows-2), sortedRowMins(numRows-3), sortedRowMins(numRows-4), sortedRowMins(numRows-5) )

   FeatureLog.info("Normalized row numZeros at rows, values:")
   FeatureLog.info(sortedRowNumZeros(0), sortedRowNumZeros(1), sortedRowNumZeros(2), sortedRowNumZeros(3), sortedRowNumZeros(4) )
   FeatureLog.info(sortedRowNumZeros(numRows-1), sortedRowNumZeros(numRows-2), sortedRowNumZeros(numRows-3), sortedRowNumZeros(numRows-4), sortedRowNumZeros(numRows-5) )

   FeatureLog.info("Normalized row normL1s at rows, values:")
   FeatureLog.info(sortedRowNormL1s(0), sortedRowNormL1s(1), sortedRowNormL1s(2), sortedRowNormL1s(3), sortedRowNormL1s(4) )
   FeatureLog.info(sortedRowNormL1s(numRows-1), sortedRowNormL1s(numRows-2), sortedRowNormL1s(numRows-3), sortedRowNormL1s(numRows-4), sortedRowNormL1s(numRows-5) )

   FeatureLog.info("Normalized row normL2s at rows, values:")
   FeatureLog.info(sortedRowNormL2s(0), sortedRowNormL2s(1), sortedRowNormL2s(2), sortedRowNormL2s(3), sortedRowNormL2s(4) )   
   FeatureLog.info(sortedRowNormL2s(numRows-1), sortedRowNormL2s(numRows-2), sortedRowNormL2s(numRows-3), sortedRowNormL2s(numRows-4), sortedRowNormL2s(numRows-5) )
   

*******************************************************************************/
/******************************************************************************/

// create a contingency matrix

   var pdLinArr = new Array[Double](numRows*numCols)

   for (i <- 0 to numRows - 1)
     for (j <- 0 to numCols - 1)
        pdLinArr(i*numCols + j) = math.abs(pdArr(i)(j))

   var mat: Matrix = Matrices.dense(numRows, numCols, pdLinArr)

// conduct Pearson's independence test on the input contingency matrix
   FeatureLog.info("Computing Pearson's independence test on the input contingency matrix using chi-square test")
   
   var independenceTestResult = Statistics.chiSqTest(mat)
   
// summary of the test including the p-value, degrees of freedom
   FeatureLog.info(s"$independenceTestResult\n")


/*******************************************************************************/
//   test for normal distribution using Kolmogorov-Smirnov test
//
   var colVec = new Array[Double](numRows)
   var vecRDD = sc.parallelize(colVec)
   var testResult = Statistics.kolmogorovSmirnovTest(vecRDD, "norm", 0, 1)   
   
   FeatureLog.info("Checking if column data is normally distributed using Kolmogorov-Smirnov test")   

   for (j <- 0 to numCols - 1)
   {
     if (typeStrings(j) != "String")
	 {
        FeatureLog.info(" " + hA(j) + " ")
        for (i <- 0 to numRows - 1)
        {
           colVec(i) = pdArr(i)(j)
        }
		vecRDD = sc.parallelize(colVec)
        testResult = Statistics.kolmogorovSmirnovTest(vecRDD, "norm", means(j), stdDev(j))
        FeatureLog.info(testResult)		
     }		
   }
/*******************************************************************************/
/*******************************************************************************
//
//   test for a distribution using Chi-square test
//
   //var colVec = new Array[Double](numRows)
   var vec: Vector = Vectors.dense(colVec)
   var goodnessOfFitTestResult = Statistics.chiSqTest(vec)
   var colVec2 = new Array[Double](numRows)
   var vec2: Vector = Vectors.dense(colVec2)
   var shift = 0.0
   
   FeatureLog.info("Checking if column data have a given distribution using Chi-square test")
   
   for (j <- 0 to numCols - 1)
   {
     if (typeStrings(j) != "String")
	   {
        FeatureLog.info(" " + hA(j) + " ")
		    shift = 0.0
		    if (mins(j) < 0.0) shift = -mins(j) 
        for (i <- 0 to numRows - 1)
        {
           colVec(i) = pdArr(i)(j) + shift
		       colVec2(i) = pdArr(i)(j) + shift + 1.0
        }
        vec = Vectors.dense(colVec)
        vec2 = Vectors.dense(colVec2)
        goodnessOfFitTestResult = Statistics.chiSqTest(vec, vec2)
	    FeatureLog.info(s"$goodnessOfFitTestResult\n")
     }		
   }
   
*******************************************************************************/
/*******************************************************************************/
//
//   estimate kernel densities
//
import org.apache.spark.mllib.stat.KernelDensity

   //var colVec = new Array[Double](numRows)
   //var vecRDD = sc.parallelize(colVec)
   
   FeatureLog.info("Computing kernel densities on all columns using a Bandwidth of 3.0")

   var kd = new KernelDensity()
               .setSample(vecRDD)
      			   .setBandwidth(3.0)
  
   //val sAS = math.sqrt(numRows).toInt                                   // sample array size
   //var samplePoints = new Array[Double](sAS)   
   var samplePoints = new Array[Double](numRows)

   //for (i <-0 to  sAS - 1)
   //  samplePoints(i) = i*sAS
   for (i <-0 to  numRows - 1)
      samplePoints(i) = i;

   var densities = kd.estimate(samplePoints)	 
   
   FeatureLog.info("Estimating kernel densities")
   
   FeatureLog.info("Print kernel densities > 0.01 at 'x-axis' points")	   
   for (j <- 0 to numCols - 1)
   {
     if (typeStrings(j) != "String")
  	 {
        FeatureLog.info(" " + hA(j) + " ")
        for (i <- 0 to numRows - 1)
        {
           colVec(i) = pdArr(i)(j)
        }
	    	vecRDD = sc.parallelize(colVec)
        kd = new KernelDensity()
		        .setSample(vecRDD)
      			.setBandwidth(3.0)
		
        // Find density estimates for the given values
        densities = kd.estimate(samplePoints)
        outStr = "[" 		
    		//for (i <-0 to  sAS - 1)
		    //   outStr = outStr + densities(i) + ","
    		//outStr += "]"
		    //FeatureLog.info(outStr)
    		for (i <-0 to  numRows - 1)
		      if (densities(i) >= 0.01) outStr = outStr + "(" + i + "," + densities(i) + "),"
      		outStr += "]"
      		FeatureLog.info(outStr) 		   
     }		
   }
/*******************************************************************************/
//
//  compute Skewness and Kurtosis for each column
//
   var skew = new Array[Double](numCols)
   var kurt = new Array[Double](numCols)
   var term = 0.0
   var outStr2 = "["
   var outStr3 = "[" 
   
   outStr = "[" 
   for (j <- 0 to numCols - 1)
   {
	  if (j != 0)
      {
        outStr += ","; outStr2 += ","; outStr3 += ","
      }		
      if (typeStrings(j) == "String")
      {
         skew(j) = 0
		 kurt(j) = 0

	     outStr = outStr + " Text "
  	     outStr2 = outStr2 + " Text "
         outStr3 = outStr3 + " Text "			 
      }
      else
      {
         skew(j) = 0
		 kurt(j) = 0

// extra work: find Ints
         typeStrings(j) = "Int"
         for (i <- 0 to numRows - 1)
		 {

            if ((typeStrings(j) == "Int") && (pdArr(i)(j).toInt.toDouble != pdArr(i)(j)))
			   typeStrings(j) = "Double"
		    term = (pdArr(i)(j) - summary.mean(j))/stdDev(j)
		    skew(j) = skew(j) + (term*term*term)
			kurt(j) = kurt(j) + (term*term*term*term)
         }
         skew(j) = skew(j)/numRows
         kurt(j) = (kurt(j)/numRows) - 3
		 
	     outStr = outStr + skew(j).toString
  	     outStr2 = outStr2 + kurt(j).toString
	     outStr3 = outStr3 + typeStrings(j)			 
      }		 
  
   }
   outStr += "]" ; FeatureLog.info("Skewness of columns");  FeatureLog.info(outStr)
   outStr2 += "]" ; FeatureLog.info("Kurtosis of columns");  FeatureLog.info(outStr2)
   outStr3 += "]"
   
   FeatureLog.info("Inferring column data types (Text string, Int, Double)")
   FeatureLog.info(outStr3) 
   
// numbers that are Int and non-negative and  "large" are likely to be numeric labels -- keep checking this heuristic
// columns that are outside Kurtosis limits <-1.2, 3.0> may be numeric labels

   FeatureLog.info("Attempting to infer if an Int column is a numeric label")
   FeatureLog.info("If all Ints in a column are >= 0 and 'large', it may be numLabel")
   FeatureLog.info("If all Ints in a column are >= 0 and excess kurtosis is outside [-1.2, 3.0], it may be numLabel")
   
   for (j <- 0 to numCols - 1)
   {
       if ((typeStrings(j) == "Int") && (mins(j) >= 0) && ((maxs(j) > 10000) || (kurt(j) < -1.2) || (kurt(j) > 3.0)))
       {
           FeatureLog.info("column " + j + " (" + hA(j) + ") " + " may be a numeric label")
           typeStrings(j) = "NumLabel"
       }
   }

   
/******************************************************************************/
//
//   Normalize the dataset by shifting by mean and scaling by stdDev
//
   var normData = ofDim[Double](numRows, numCols)
   var rowMaxs , rowMins, rowNormL1s, rowNormL2s = new Array[Double](numRows)
   var rowNumZeros = new Array[Int](numRows)


   for (i <- 0 to numRows -1)
   {
     rowMaxs(i) = -999999.0
  	 rowMins(i) = 999999.0
	   rowNumZeros(i) = 0
  	 rowNormL1s(i) = 0.0
  	 rowNormL2s(i) = 0.0
	 
     for ( j <- 0 to numCols - 1 )
     {
	     if ((typeStrings(j) == "Int") || (typeStrings(j) == "Double"))
	  	 {
        normData(i)(j) = (pdArr(i)(j) - means(j))/stdDev(j)
		    if (normData(i)(j) > rowMaxs(i)) rowMaxs(i) = normData(i)(j)
		    if (normData(i)(j) < rowMins(i)) rowMins(i) = normData(i)(j)
		    if (normData(i)(j) == 0.0) rowNumZeros(i) += 1
        if (math.abs(normData(i)(j)) < 100.0)
		    {
		       rowNormL1s(i) += math.abs(normData(i)(j))
		       rowNormL2s(i) += normData(i)(j)*normData(i)(j)
        }
      }			
     }
   }
   
// output a .csv

import java.io.PrintWriter

   var fileHandle = new PrintWriter(outputFileName)

   // Add Header
   if (numCols > 1)
   {
      for (j <- 0 to math.max(0 , numCols - 2))
      {
          fileHandle.print( hA(j)); fileHandle.print(",")	  
	  }
   }
   fileHandle.println( hA(numCols - 1))   


   val parsedStringData = inpFileNh.flatMap(s => s.split(','))   // parsedStringData is an RDD
   val pSA = parsedStringData.collect()                        // pSA is an Array of String   

   for (i <- 0 to numRows - 1)
   {
     if (numCols > 1)
     {
        for (j <- 0 to math.max(0 , numCols - 2))
        {
           if ((typeStrings(j) == "String") || (typeStrings(j) == "NumLabel"))
	         {
		      // copy input to output
              fileHandle.print( pSA(numCols*i + j)); fileHandle.print(",")
           }
           else
           {
              fileHandle.print(String.valueOf(normData(i)(j)))
              fileHandle.print(",")
		       }
        }
     }	 

     if ((typeStrings(numCols - 1) == "String") || (typeStrings(numCols - 1) == "NumLabel"))
     {
	    // copy input to output
  		fileHandle.println( pSA(numCols*i + numCols - 1))
     }
     else
     {
        fileHandle.println(String.valueOf(normData(i)(numCols - 1)))
     }		
   }

   fileHandle.close()

   FeatureLog.info("Wrote " + outputFileName)
   
// Create a .csv file for use in "Labeling" on the gateway

   var lastIndexOfSlash = inputFileName.dropRight(4).lastIndexOf('/')
   
   var labelFileName = inputFileName.dropRight(4).drop(lastIndexOfSlash+1) + "Label.csv"
   
   

   fileHandle = new PrintWriter(labelFileName)
   
   // Keep upto 6 columns of identifying info
   if (numCols > 1)
   {
      for (j <- 0 to math.min(5 , numCols - 1))
      {
          fileHandle.print( hA(j)); fileHandle.print(",")	  
	    }
   }
   fileHandle.print( "L1-Norm" ); fileHandle.print(",")
   fileHandle.println( "L2-Norm" )
   
   for (i <- 0 to numRows - 1)
   {
      for (j <- 0 to math.min(5 , numCols - 1))
      {
         // copy input to output
         fileHandle.print( pSA(numCols*i + j)); fileHandle.print(",")
      }
	  fileHandle.print(rowNormL1s(i)); fileHandle.print(",")
	  fileHandle.println(math.sqrt(rowNormL2s(i)))
   }

   fileHandle.close()

   FeatureLog.info("Wrote " + "label" + outputFileName)   

   

   
   
// print end and middle values

   var sortedRowMaxs = new Array[Tuple2[Int, Double]](numRows)
   var sortedRowMins = new Array[Tuple2[Int, Double]](numRows)
   var sortedRowNumZeros = new Array[Tuple2[Int, Int]](numRows)   
   var sortedRowNormL1s = new Array[Tuple2[Int, Double]](numRows)
   var sortedRowNormL2s = new Array[Tuple2[Int, Double]](numRows)   

   for (i <- 0 to numRows - 1)
   {
      sortedRowMaxs(i) = (i, rowMaxs(i))
      sortedRowMins(i) = (i, rowMins(i))
      sortedRowNumZeros(i) = (i, rowNumZeros(i))
      sortedRowNormL1s(i) = (i, rowNormL1s(i))
      sortedRowNormL2s(i) = (i, rowNormL2s(i))	  
	  
   }

   scala.util.Sorting.stableSort(sortedRowMaxs, (e1: (Int, Double), e2: (Int, Double)) => e1._2 < e2._2)
   scala.util.Sorting.stableSort(sortedRowMins, (e1: (Int, Double), e2: (Int, Double)) => e1._2 < e2._2)
   scala.util.Sorting.stableSort(sortedRowNumZeros, (e1: (Int, Int), e2: (Int, Int)) => e1._2 < e2._2)
   scala.util.Sorting.stableSort(sortedRowNormL1s, (e1: (Int, Double), e2: (Int, Double)) => e1._2 < e2._2)
   scala.util.Sorting.stableSort(sortedRowNormL2s, (e1: (Int, Double), e2: (Int, Double)) => e1._2 < e2._2)
   
   FeatureLog.info("Normalized row maxes at rows, values:")
   FeatureLog.info(sortedRowMaxs(0), sortedRowMaxs(1), sortedRowMaxs(2), sortedRowMaxs(3), sortedRowMaxs(4) )
   FeatureLog.info(sortedRowMaxs(numRows-1), sortedRowMaxs(numRows-2), sortedRowMaxs(numRows-3), sortedRowMaxs(numRows-4), sortedRowMaxs(numRows-5) )
   
   FeatureLog.info("Normalized row mins at rows, values:")
   FeatureLog.info(sortedRowMins(0), sortedRowMins(1), sortedRowMins(2), sortedRowMins(3), sortedRowMins(4) )
   FeatureLog.info(sortedRowMins(numRows-1), sortedRowMins(numRows-2), sortedRowMins(numRows-3), sortedRowMins(numRows-4), sortedRowMins(numRows-5) )

   FeatureLog.info("Normalized row numZeros at rows, values:")
   FeatureLog.info(sortedRowNumZeros(0), sortedRowNumZeros(1), sortedRowNumZeros(2), sortedRowNumZeros(3), sortedRowNumZeros(4) )
   FeatureLog.info(sortedRowNumZeros(numRows-1), sortedRowNumZeros(numRows-2), sortedRowNumZeros(numRows-3), sortedRowNumZeros(numRows-4), sortedRowNumZeros(numRows-5) )

   FeatureLog.info("Normalized row normL1s at rows, values:")
   FeatureLog.info(sortedRowNormL1s(0), sortedRowNormL1s(1), sortedRowNormL1s(2), sortedRowNormL1s(3), sortedRowNormL1s(4) )
   FeatureLog.info(sortedRowNormL1s(numRows-1), sortedRowNormL1s(numRows-2), sortedRowNormL1s(numRows-3), sortedRowNormL1s(numRows-4), sortedRowNormL1s(numRows-5) )

   FeatureLog.info("Normalized row normL2s at rows, values:")
   FeatureLog.info(sortedRowNormL2s(0), sortedRowNormL2s(1), sortedRowNormL2s(2), sortedRowNormL2s(3), sortedRowNormL2s(4) )   
   FeatureLog.info(sortedRowNormL2s(numRows-1), sortedRowNormL2s(numRows-2), sortedRowNormL2s(numRows-3), sortedRowNormL2s(numRows-4), sortedRowNormL2s(numRows-5) )
   

/******************************************************************************/   
  
// compute median for each column

   val medians = new Array[Double](numCols)
   var aCol = new Array[Double](numRows)

   outStr = "["
   for(j <- 0 to numCols - 1)
   {
      for (i <- 0 to numRows - 1)
	  {
	     aCol(i) = pdArr(i)(j)
      }
      scala.util.Sorting.quickSort(aCol)
	  
	  if (j != 0) outStr += ","
	  
      if (typeStrings(j) == "String")
      {
         medians(j) = 0
	     outStr = outStr + " Text "		 
      }
	  else
	  {
	     medians(j) = aCol(numRows/2)
	     if ((typeStrings(j) == "NumLabel") || (typeStrings(j) == "Int") )
		    outStr = outStr + " " + medians(j).toInt.toString + " "
         else			
	        outStr = outStr + medians(j).toString		 
      }		 

   }
   outStr += "]" ; FeatureLog.info("medians");  FeatureLog.info(outStr)
   
// compute histograms for each column

   val numBins = Math.sqrt(numRows).toInt
   var histogram = new Array[Int](numBins+1)
   var binWidth = 0.0
  
   
   FeatureLog.info("Computing histograms for numeric columns")
   FeatureLog.info("choosing " + numBins.toString + " bins");
   
   for (j <- 0 to numCols - 1)
   {
     if (typeStrings(j) == "String")
	 {
		FeatureLog.info( "column " + j + " (" + hA(j) + ") " + "Text" )
     }
     else
     {	 
        binWidth = (summary.max(j) - summary.min(j))/numBins
	    for (i <- 0 to numBins)
	       histogram(i) = 0
        for (i <- 0 to numRows - 1)
	    {
	       histogram(((pdArr(i)(j) - summary.min(j))/binWidth).toInt) += 1
        }
		outStr = "column " + j.toString + " (" + hA(j).toString + ") "
		if (typeStrings(j) == "NumLabel")
		   outStr = outStr + "NumLabel "
	    for (i <- 0 to numBins)
	    {
		   outStr = outStr + histogram(i).toString + ","
        }
		FeatureLog.info(outStr)
     }		   
   }

// compute modes

   var modes = new Array[Double](numCols)
   var largestBin = 0
   var binIndex = 0
   
   FeatureLog.info("modes:")
   outStr = "[ "
   for (j <- 0 to numCols - 1)
   {
     if (typeStrings(j) == "String")
	 {
	    modes(j) = 0
        outStr = outStr + "Text "		
     }
     else
     {
	    largestBin = 0
		binIndex = 0
	    for (i <- 0 to numBins)
        {
           // pick the bin with most items
           if (histogram(i) > largestBin)
              binIndex = i
        }
        modes(j) = summary.min(j) + (summary.max(j) - summary.min(j))*binIndex/numBins.toDouble
		
	    if ((typeStrings(j) == "NumLabel") || (typeStrings(j) == "Int") )
		   outStr = outStr + " " + modes(j).toInt.toString + " "
        else
		   outStr = outStr + modes(j).toString + " "
     }
    }
	outStr = outStr + " ]"
	FeatureLog.info(outStr)


/******************************************************************************/
/*******************************************************************************
// Identify possibly missing/inlier/outlier data using user selected filter
// Display both row major (i.e. inlier/outlier items) and column major (inlier/outlier measurement process)
// print out inliers/outliers 
//
      
   var processedData = ofDim[Double](numRows, numCols)
   
   var IntervalOptionUC = IntervalOption.toUpperCase
   
   if ((IntervalOptionUC != "INLIER") && (IntervalOptionUC != "OUTLIER"))
   {
      FeatureLog.error("Found Interval Option " + IntervalOption)
      FeatureLog.error("""Valid Interval options: "Inlier" or "Outlier""")
      FeatureLog.warn("Using IntervalOption Inlier")
	  IntervalOptionUC = "INLIER"
   }
   else
   {
      FeatureLog.info("Using IntervalOption " +  IntervalOption)
   }   

   if (IntervalOptionUC == "INLIER")
   {
      FeatureLog.info("Identifying possibly inlier data using user defined filter")
      FeatureLog.info("Displaying both row major (inlier items) and column major (inlier processes)")   
   }
   else
   {
      FeatureLog.info("Identifying possibly outlier data using user defined filter")
      FeatureLog.info("Displaying both row major (outlier items) and column major (outlier processes)")
   }	  

   outStr = "["  ; FeatureLog.info("Column headers")
   for (j <- 0 to numCols -1)
   {
	  if (j != 0) outStr += ","
	  outStr = outStr + hA(j).toString   
   }
   outStr += "]" ; FeatureLog.info(outStr) 
   
   // print out origDataFile rows whenever ANY field is inside/outside [UserSelectedLowerLimit, UserSelectedUpperLimit]
   var USLL = UserSelectedLowerLimit
   var USUL = UserSelectedUpperLimit
   if ((IntervalOptionUC == "INLIER") && (UserSelectedLowerLimit > UserSelectedUpperLimit))
   {
      FeatureLog.warn("Lower limit " + UserSelectedLowerLimit + " is greater than upper limit " + UserSelectedUpperLimit)
	  FeatureLog.warn("Choosing: Lower limit = 0.2 and upper limit = 0.8")
	  USLL = 0.2
	  USUL = 0.8
   }
   
   if ((IntervalOptionUC == "OUTLIER") && (UserSelectedLowerLimit > UserSelectedUpperLimit))
   {
      FeatureLog.warn("Lower limit " + UserSelectedLowerLimit + " is greater than upper limit " + UserSelectedUpperLimit)
	  FeatureLog.warn("Choosing: Lower limit = -3.0 and upper limit = 3.0")
	  USLL = -3.0
	  USUL = 3.0
   }   

   if (IntervalOptionUC == "INLIER")   
      FeatureLog.info("Detecting inlier data using [ " + USLL + "," + USUL + " ] filter")
   else
      FeatureLog.info("Detecting outlier data using [ " + USLL + "," + USUL + " ] filter")      
   
   var j = 0
   var found = false

   var param = 0.0
   var lowlim = 0.0
   var uplim = 0.0
   var deviation = 0.0

   // detect feature rows

   FeatureLog.info("Row major(item) scan")
   
   var FeatureOptionUC = FeatureOption.toUpperCase
   
   if ((FeatureOptionUC != "ZERO") && (FeatureOptionUC != "MEAN") && (FeatureOptionUC != "MEDIAN") && (FeatureOptionUC != "MODE") && (FeatureOptionUC != "NONE"))
   {
      FeatureLog.error("Found Feature Option " + FeatureOption)
      FeatureLog.error("""Valid Feature options: "Zero", "Mean", "Median", "Mode" and "None""")
      FeatureLog.warn("Using FeatureOption Mean")
	  FeatureOptionUC = "MEAN"
   }
   else
   {
      FeatureLog.info("Using FeatureOption " +  FeatureOption)
   }	  

   for ( i <- 0 to numRows - 1)
   {
      for (j <- 0 to numCols -1 )
      {
         if ((typeStrings(j) == "Int")  || (typeStrings(j) == "Double"))
		 {
            param = pdArr(i)(j)
		    if (IntervalOptionUC == "INLIER")
			{
               lowlim = mins(j) + USLL*(maxs(j) - mins(j))
			   uplim = mins(j) + USUL*(maxs(j) - mins(j))
            }
            else
            {
               FeatureOptionUC match 
               {
                   case "ZERO"   => { lowlim = USLL*stdDev(j) ; uplim = USUL*stdDev(j) }
				   case "MEAN"   => { lowlim = means(j) + USLL*stdDev(j) ; uplim = means(j) + USUL*stdDev(j) }
				   case "MEDIAN" => { lowlim = medians(j) + USLL*stdDev(j) ; uplim = medians(j) + USUL*stdDev(j) }
				   case "MODE"   => { lowlim = modes(j) + USLL*stdDev(j) ; uplim = modes(j) + USUL*stdDev(j) }
				   case "NONE"   => { lowlim = mins(j) ; uplim = maxs(j) }
               }				   
            }
		
            if (((IntervalOptionUC == "INLIER") && (param > lowlim) && (param < uplim)) ||
			     ((IntervalOptionUC == "OUTLIER") && ((param < lowlim) || (param > uplim)))  )
// seek out "inliers" in a bathtub curve distribution or outliers in a bellcurve distribution
	        {
		        if (IntervalOptionUC == "INLIER")
				{
				   // find deviation on linear scale
				   deviation = (param - mins(j))/(maxs(j) - mins(j))
			       FeatureLog.warn("( " + i + " , " + j + " ): " + param + " [ " + lowlim + " to " + uplim + " ] " + " off by " + deviation + " of full range")				   
                }
                else
                {				
		           // find deviation in sigmas
			       FeatureOptionUC match 
                   {
                     case "ZERO"   => deviation = param/stdDev(j)
                     case "MEAN"   => deviation = (param - means(j))/stdDev(j)
			         case "MEDIAN" => deviation = (param - medians(j))/stdDev(j)
			         case "MODE"   => deviation = (param - modes(j))/stdDev(j)
			         case "NONE"   => deviation = 0
                   }					   
			       FeatureLog.warn("( " + i + " , " + j + " ): " + param + " [ " + lowlim + " to " + uplim + " ] " + " off by " + deviation + " Sigmas")				   
                }				   
	       }
        }		 
      }
   }

*******************************************************************************/   
   // detect feature columns
/*******************************************************************************
   FeatureLog.info("Column major(process) scan")

   for ( j <- 0 to numCols - 1)
   {
      if ((typeStrings(j) == "Int")  || (typeStrings(j) == "Double"))   
      for ( i <- 0 to numRows -1 )
      {
        param = pdArr(i)(j)
	    if (IntervalOptionUC == "INLIER")
		{
           lowlim = mins(j) + USLL*(maxs(j) - mins(j))
           uplim = mins(j) + USUL*(maxs(j) - mins(j))
        }
        else
        {			
           FeatureOptionUC match 
           {
               case "ZERO"   => { lowlim = USLL*stdDev(j) ; uplim = USUL*stdDev(j) }
               case "MEAN"   => { lowlim = means(j) + USLL*stdDev(j) ; uplim = means(j) + USUL*stdDev(j) }
			   case "MEDIAN" => { lowlim = medians(j) + USLL*stdDev(j) ; uplim = medians(j) + USUL*stdDev(j) }
			   case "MODE"   => { lowlim = modes(j) + USLL*stdDev(j) ; uplim = modes(j) + USUL*stdDev(j) }
			   case "NONE"   => { lowlim = mins(j) ; uplim = maxs(j) }
           }
        }			 
	  
        if (((IntervalOptionUC == "INLIER") && (param > lowlim) && (param < uplim)) ||
            ((IntervalOptionUC == "OUTLIER") && ((param < lowlim) || (param > uplim)))  )
        {			
           if (IntervalOptionUC == "INLIER")
		   {
		      // find deviation on linear scale
		      deviation = (param - mins(j))/(maxs(j) - mins(j))
	          FeatureLog.warn("( " + i + " , " + j + " ): " + param + " [ " + lowlim + " to " + uplim + " ] " + " off by " + deviation + " of full range")				   
           }
           else
           {				
              // find deviation in sigmas
			  FeatureOptionUC match 
              {
                  case "ZERO"   => deviation = param/stdDev(j)
                  case "MEAN"   => deviation = (param - means(j))/stdDev(j)
			      case "MEDIAN" => deviation = (param - medians(j))/stdDev(j)
			      case "MODE"   => deviation = (param - modes(j))/stdDev(j)
			      case "NONE"   => deviation = 0
              }			  
	          FeatureLog.warn("( " + i + " , " + j + " ): " + param + " [ " + lowlim + " to " + uplim + " ] " + " off by " + deviation + " Sigmas")				   
           }		 
        }
      }		
   }

*******************************************************************************/

   
   return 0;

   } // findFeatures()


  var rc = -1
  
   def main(args : Array[String]) = {

// Kept above for visibility
///      val MainLog = LogManager.getRootLogger
///      val FeatureLog = LogManager.getLogger("FeatureLogger")

// Log level hierarchy		  
// WARN includes ERROR and FATAL
// ALL includes TRACE, DEBUG, INFO, WARN, ERROR, FATAL
// use OFF to turn everything off
//		  

import java.io.IOException

    try
	{
	
      MainLog.setLevel(Level.WARN)
	  
      FeatureLog.setLevel(Level.ALL)
	  
      FeatureLog.info("Finding features using findFeatures")

	  
	  if (args.length < 2)
	     FeatureLog.error("Too few arguments; need input directory and output directory paths")
	  else
	     FeatureLog.info("input = " + args(0) + " output = " + args(1))
		 
	  rc = findFeatures(args(0), args(1), "Outlier", -3.0, 3.0, "Median")
      	  
	  FeatureLog.info("findFeatures ended with " + rc)
    	  
	}
	catch
	{
       case ex: IOException  => FeatureLog.info("findFeatures threw " + ex.toString)
	}
	finally
	{
       FeatureLog.info("findFeatures finally ended")
	}	
	
  }  // main
}  // features