import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import scala.util.control.Exception._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseMatrix
import java.util.Random
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import Array._
import java.security.MessageDigest
import java.io.IOException
import java.io.PrintWriter


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
   def with0Str(s : String) : String = {
      if ((allCatch.opt(s.toDouble)).isDefined == true)
	 return s
      else
         return ("0")
   }
}  // replaceText	


object AnonCharsDigits {
//  
// This function takes a string and generates a similar looking anonymized string
//
   def makeAnonStr(Str : String) : String = 
   {
      var AoC = new Array[Char](Str.length)
      var md5AoB = new Array[Byte](16)
      var substrEnd : Int = 0
      var myInt : Int = 0
      var c : Char = ' '
   
      // To Do: date fields should become valid dates.

      for (i <- 0 to Str.length - 1)
      {
         if (i % 16 == 0)
	 {
            substrEnd = i + 16
	    if (i + 16 > Str.length)
               substrEnd =  Str.length
               md5AoB = MessageDigest.getInstance("MD5").digest(Str.substring(i, substrEnd).getBytes)
         }		 
   
         c = Str.charAt(i)
	  
	 AoC(i) = c
	 myInt = md5AoB(i % 16) + 128
	  
         if (c >= 'A' && c <= 'Z')
	    AoC(i) = ((myInt % 26) + 'A'.toInt).toChar
		
         if (c >= 'a' && c <= 'z')
	    AoC(i) = ((myInt % 26) + 'a'.toInt).toChar

         if (c >= '0' && c <= '9')
	    AoC(i) = ((myInt % 10) + '0'.toInt).toChar
		
      }
      return(AoC.mkString(""))
   }
}

object GlobalVars {

   val MAXCOLS = 100
   
   var stdDev = new Array[Double](MAXCOLS)
   var means = new Array[Double](MAXCOLS)
   var typeStrings = new Array[String](MAXCOLS)
}

object AnonRow  {
   def withNiceStrAndNormals(sAin : Array[String]) : Array[String] = {
   
      var sAout = new Array[String](sAin.length)
      var d = 0.0;
      
      for (j <- 0 to sAin.length - 2) // skip the sentinel
      {
         if (GlobalVars.typeStrings(j) == "String")
            sAout(j) = AnonCharsDigits.makeAnonStr(sAin(j))
         else
         {
            d = (scala.util.Random.nextGaussian()*GlobalVars.stdDev(j) + GlobalVars.means(j))
            if (GlobalVars.typeStrings(j) == "Double")
               sAout(j) = d.toString
            else
               sAout(j) = d.toInt.toString
         }               
      }            
            
      return sAout
   }
}  // AnonRow			  

object Anonymizer {

   val MainLog = LogManager.getRootLogger
   val AnonLog = LogManager.getLogger("AnonLogger")
   
   val conf = new SparkConf().setAppName("Anonymizer")
   val sc = new SparkContext(conf)  	  
	  

   def makeSubstData(inputFileName : String, outputFileName : String) : Int =
   { 
   // Read from the input file an array of alfanumeric labels and numbers, compute the mean and variance of numbers and then
   // generate a new array of normally distributed random numbers
   // Compute the mean and variance (for verification) and write a .csv file with the generated data
   // Anonymize alfanumeric labels

   // read inputFile to a vector
   // inputFile will be RDD[String]
   
      var inpFile = sc.textFile(inputFileName)

      var numRows = inpFile.count().toInt

      AnonLog.info("Read " + inputFileName + ", Read " + numRows + " Rows")

      AnonLog.trace("print out a few rows read from file")
      inpFile.take(5).foreach(AnonLog.trace)
   
   
//////////////////////////////////////////////////////////////////////////////////////////
//    Rectangularize the RDD, if needed before vectorizing
//

//    filter element to remove quotes to prevent (quote) embedded commas
      val inpFileUq = inpFile.map(s => unQuote.removeEmbeddedCommas(s) )
//    inpFileUq.take(5).foreach(AnonLog.trace)

      val countFields = inpFileUq.map(s => s.split(',').length).collect()
      var RectangularizationNeeded = false
  
      // AnonLog.info("number of fields in each row: ")

      var maxCount = 0
      var maxCountAt = 0
  
      var outStr : String = "[" 
  
      for (i <- 0 to numRows - 1)
      {
      if (countFields(i) > maxCount)
      {
	 maxCount = countFields(i); maxCountAt = i
      }
	
	 // outStr = outStr + countFields(i).toString + ","
         if ((RectangularizationNeeded == false) && (i > 0))
	 {
	    if (countFields(i) != countFields(i-1))
	    {
	       RectangularizationNeeded = true
	    }
         }	   
      }	
      // outStr += "]" ; AnonLog.trace(outStr)   
  
      if (RectangularizationNeeded == true)
	 AnonLog.info("Identified jagged data set; Rectangularization needed")
      else
	 AnonLog.info("Identified rectangular data set; No fixup needed")

//    infer number of columns by picking the largest of the top few

      AnonLog.info("Inferring longest row(s) has " + maxCount + " fields at row " + maxCountAt)
  
//    rectangularize the RDD to numRows X maxCount

      var inpFileRe = inpFileUq.map(s => s + ",No Data")
      // remove short rows
      val shortFile = inpFileRe.filter(row => row.split(',').length < maxCount+1)
      AnonLog.trace("Short rows will be filtered out")		
      shortFile.take(40).foreach(AnonLog.trace)		  
      // truncate to maxCount+1 columns
      val inpFileTr = inpFileRe.filter(row => row.split(',').length == maxCount + 1)

      var header = inpFileTr.first()              // header is a String
      // split header into array of column header strings

      var hA = header.split(',')                 // hA is an Array of Strings

      var inpFileNh = inpFileTr.filter(row => row != header)

      AnonLog.trace("Removed the First row as Header")
      numRows = inpFileNh.count().toInt
      AnonLog.trace("number of rows = " + numRows)   

/////////////////////////////////////////////////////////////////////////////////////////   
   
  // parsedData will be org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vector]

      val parsedData = inpFileNh.map(s => Vectors.dense(s.split(',').map(replaceText.with0Str(_)).map(_.toDouble)))


      AnonLog.trace("print out a few vectors after converting from strings")
      parsedData.take(5).foreach(AnonLog.trace)   

      val summary: MultivariateStatisticalSummary = Statistics.colStats(parsedData)

      AnonLog.info("print out summary statistics for each column")
      AnonLog.info("summary.mean");  AnonLog.info(summary.mean)
      AnonLog.info("summary.variance");  AnonLog.info(summary.variance)
      AnonLog.info("summary.count");  AnonLog.info(summary.count)
      AnonLog.info("summary.max");  AnonLog.info(summary.max)
      AnonLog.info("summary.min");  AnonLog.info(summary.min)
      AnonLog.info("summary.normL1");  AnonLog.info(summary.normL1)
      AnonLog.info("summary.normL2");  AnonLog.info(summary.normL2)
      AnonLog.info("summary.numNonzeros");  AnonLog.info(summary.numNonzeros)
   
      var numCols = summary.mean.size   
   
      GlobalVars.stdDev = new Array[Double](numCols)

      for (j <- 0 to numCols - 1)
         GlobalVars.stdDev(j) = Math.sqrt(summary.variance(j))   

      GlobalVars.means = summary.mean.toArray
      var variances = summary.variance.toArray
      var normL1s = summary.normL1.toArray
      var normL2s = summary.normL2.toArray
      var maxs = summary.max.toArray
      var mins = summary.min.toArray

      GlobalVars.typeStrings = new Array[String](numCols)

      // infer columns where normL1, normL2, mean, variance, max and mean are 0 as non-numeric

      AnonLog.info("Inferring column data types")
   
      outStr = "["
   
      for (j <- 0 to numCols -2) // skip the sentinel
      {
         if ((normL1s(j) == 0) && (normL2s(j) == 0) && (GlobalVars.means(j) == 0) && (variances(j) == 0) && (maxs(j) == 0) && (mins(j) == 0))
         {
            GlobalVars.typeStrings(j) = "String"
         }	  
         else
         {
	    if ((normL1s(j).toInt.toDouble == normL1s(j)) && (maxs(j).toInt.toDouble == maxs(j)) && (mins(j).toInt.toDouble == mins(j)) )
	    {
	       GlobalVars.typeStrings(j) = "Int"
            }		 
            else
	    {
               GlobalVars.typeStrings(j) = "Double"	  
            }
         }
	 if (j != 0) outStr += ","
	    outStr = outStr + GlobalVars.typeStrings(j)	  
      }
   
      outStr += "]" ; AnonLog.info(outStr)      

      val rng = new Random

      val parsedStringData = inpFileNh.map(s => s.split(',')).map(AnonRow.withNiceStrAndNormals(_))
      
      val arStrings = parsedStringData.take(numRows)
      
      AnonLog.info("using means and variances of:")

      AnonLog.info("means:")
   
      outStr = "["
   
      for (j <- 0 to numCols - 1)
      {
         if (j != 0) outStr += ","   
         outStr = outStr + GlobalVars.means(j).toString
      }	  
      outStr += "]" ; AnonLog.info(outStr)   	  
	  

      AnonLog.info("variances:")
      outStr = "["  
   
      for (j <- 0 to numCols - 1)
      {
         if (j != 0) outStr += ","    
         outStr = outStr + variances(j).toString
      }	  
      outStr += "]" ; AnonLog.info(outStr)

/*******************************************************************************/
//    This is for Single machine use only!
//   

      val fileHandle = new PrintWriter(outputFileName)

      if (numCols > 2)
      {
         for (j <- 0 to math.max(0 , numCols - 3))
         {
	    // Don't anonymize header
            fileHandle.print( hA(j)); fileHandle.print(",")
	 }
      }
      fileHandle.println( hA(numCols - 2))
   

      for (i <- 0 to numRows - 1)
      {
         if (numCols > 2)
         {
            for (j <- 0 to math.max(0 , numCols - 3))
            {
                  fileHandle.print( arStrings(i)(j)); fileHandle.print(",")
            }
         }	 

         fileHandle.println( arStrings(i)(numCols -2))
      }

      fileHandle.close()
   
      AnonLog.info("Wrote file " + outputFileName)   

/*******************************************************************************/
/******************************************************************************
//
//    On hdfs, write using saveAsTextFile(path)

   
      outpFile.repartition(1).saveAsTextFile(outputFileName)   // repartition(1) is coalesce(1,true)
   
      AnonLog.info("Wrote directory " + outputFileName)

      // read outputFile to a vector

      // outputFile will be RDD[String]

      var substDataFile = sc.textFile(outputFileName + "/part-00000")

      AnonLog.trace("Read " + outputFileName + "/part-00000")
   
*******************************************************************************/

      var substDataFile = sc.textFile(outputFileName)

      AnonLog.trace("Read " + outputFileName)

      AnonLog.trace("print out a few rows read from file")
      substDataFile.take(5).foreach(AnonLog.trace)

      // remove a header row
      header = substDataFile.first()

      substDataFile = substDataFile.filter(row => row != header)

      // parsedSubstData will be org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vector]

      val parsedSubstData = substDataFile.map(s => Vectors.dense(s.split(',').map(replaceText.with0Str(_)).map(_.toDouble)))

      AnonLog.trace("print out a few vectors after converting from strings")
      parsedSubstData.take(5).foreach(AnonLog.trace)

      val substDataSummary: MultivariateStatisticalSummary = Statistics.colStats(parsedSubstData)

      AnonLog.info("print out summary statistics for each column")

      AnonLog.info("substDataSummary.mean"); AnonLog.info(substDataSummary.mean)
      AnonLog.info("substDataSummary.variance"); AnonLog.info(substDataSummary.variance)
      AnonLog.info("substDataSummary.count"); AnonLog.info(substDataSummary.count)   
      AnonLog.info("substDataSummary.max"); AnonLog.info(substDataSummary.max)   
      AnonLog.info("substDataSummary.min"); AnonLog.info(substDataSummary.min)   
      AnonLog.info("substDataSummary.normL1"); AnonLog.info(substDataSummary.normL1)
      AnonLog.info("substDataSummary.normL2"); AnonLog.info(substDataSummary.normL2)
      AnonLog.info("substDataSummary.numNonzeros"); AnonLog.info(substDataSummary.numNonzeros)   
   
      return 0;
   }  // makeSubstData()
   
   var rc = -1
  
   def main(args : Array[String]) = {

//    Log level hierarchy		  
//    WARN includes ERROR and FATAL
//    ALL includes TRACE, DEBUG, INFO, WARN, ERROR, FATAL
//    use OFF to turn everything off
//
      try
      {
         MainLog.setLevel(Level.WARN)
         AnonLog.setLevel(Level.ALL)
	  
         AnonLog.info("Anonymizing using makeSubstData")

	 if (args.length < 2)
	    AnonLog.error("Too few arguments; need input directory/file and output directory/file paths")
	 else
	    AnonLog.info("input = " + args(0) + " output = " + args(1))
		 
	 rc = makeSubstData(args(0), args(1))
      	  
	 AnonLog.info("makeSubstData ended with " + rc)
	  
	 AnonLog.info("On Windows, you may now see a series of exceptions; it is a known issue in spark; See SPARK-12216:  Spark failed to delete temp directory: https://issues.apache.org/jira/browse/SPARK-12216")
    	  
      }
      catch
      {
         case ex: IOException  => AnonLog.info("makeSubstData threw " + ex.toString)
      }
      finally
      {
         AnonLog.info("makeSubstData finally ended")
      }	
	
   }  // main
}  // Anonymizer