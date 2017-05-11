/*******************************************************************************
//
// returns a list of files in the input directory that have not been previously processed
// and saved in output directory
//
   
class cs_purlFileOp extends Serializable {

    import scala.util.{Try, Failure, Success}
    import org.apache.hadoop.fs.{FileSystem,Path}
    import scala.util.control.Exception.allCatch
    import scala.collection.mutable._
    import scala.collection.immutable._
    import java.security.MessageDigest
    import scala.util._
    import scala.math._


    def getHDFSfiles (srcFolder: String = "", destFolder: String = "", Extension: String = "") = { 
    
    /*  Examples:
    fileOp.getHDFSfiles("hdfs://hddev1ns/appdata/ezone/cqn_mve_stg/staging/grindIn","csv")
    fileOp.getHDFSfiles("hdfs://hddev1ns/appdata/ezone/cqn_mve_stg/staging/grindIn","hdfs://hddev1ns/appdata/ezone/cqn_mve_stg/staging/grindOut", "csv")
    */

    try {
    
    // empty ListBuffers for found files
    val filesSrc = new ListBuffer[String]()
    val filesDest = new ListBuffer[String]()   
    val goodList = new ListBuffer[String]()
    val doneList = new ListBuffer[String]() 
    val filesFinal = new ListBuffer[String] ()
	val scN = new SparkContext()
            
    // get Source List
    if (srcFolder.isEmpty == false) {                       
        val fSSrc = FileSystem.get(sc.hadoopConfiguration).listStatus(new Path(srcFolder))
      
        for (i <- 0 until fSSrc.length) { 
            if ((fSSrc(i).getPath).toString.endsWith(Extension) == true) { filesSrc += (fSSrc(i).getPath).toString }
          }
      }
    
    // get Destination List  
    
      if (destFolder.isEmpty == false) {
        val fSDest = FileSystem.get(sc.hadoopConfiguration).listStatus(new Path(destFolder))
      
        for (i <- 0 until fSDest.length) { 
            if ((fSDest(i).getPath).toString.endsWith(Extension) == true) { filesDest += (fSDest(i).getPath).toString }
          }
        } 
        
     if (srcFolder.isEmpty == false && destFolder.isEmpty == true)  
         {filesSrc.toList}
     else if (srcFolder.isEmpty == true && destFolder.isEmpty == false )
         {filesDest.toList}
     else if (srcFolder.isEmpty == false && destFolder.isEmpty == false ) {
           for (j <- 0 until filesSrc.length)
               goodList += ( filesSrc(j).split("/") ((filesSrc(j).split("/").length)-1) )
           for (k <- 0 until filesDest.length) 
               doneList += ( filesDest(k).split("/") ((filesDest(k).split("/").length)-1) )

           val toDoList = goodList diff doneList
           
           for (m <- 0 until (filesSrc.length))
               if ( toDoList.contains(( filesSrc(m).split("/") ((filesSrc(m).split("/").length)-1))) )
               filesFinal += filesSrc(m)
           
           filesFinal.toList  
                               
         }
         // val diffList = srcL.diff(destL) 
         // diffList.toList
     }   // end of try
     catch { 
       case e: Exception => println (e)
		}
        
    }   // end of def 'getHDFSfiles'
}    // end of class

*******************************************************************************/

/******************************************************************************/
//
//  Date code to compile with version 1.6 of Spark
//
/******************************************************************************/
class cs_DateOps extends Serializable {

	import java.text.ParseException
	import java.util.Calendar
	import java.util.Date
	import java.text.SimpleDateFormat


	val df_ISO = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
	val df_Short = new SimpleDateFormat("MM-dd-yy")
	val df_USMini = new SimpleDateFormat("M-d-yy")
	val df_USMini4 = new SimpleDateFormat("M-d-yyyy")
	val df_ANSI = new SimpleDateFormat("yyyy-MM-dd")
	val df_EUShort = new SimpleDateFormat("dd-MM-yy")
	val df_EULong = new SimpleDateFormat("dd-MM-yyyy")


	def nowStamp = {
		val now = Calendar.getInstance()
		val yr = now.get(Calendar.YEAR).toString
		val mo = (now.get(Calendar.MONTH).toString).reverse.padTo(2,"0").reverse.mkString("")
		val dom = (now.get(Calendar.DAY_OF_MONTH).toString).reverse.padTo(2,"0").reverse.mkString("")
		val hod = (now.get(Calendar.HOUR_OF_DAY).toString).reverse.padTo(2,"0").reverse.mkString("")
		val minit = (now.get(Calendar.MINUTE).toString).reverse.padTo(2,"0").reverse.mkString("")
		val sec = (now.get(Calendar.SECOND).toString).reverse.padTo(2,"0").reverse.mkString("")
		yr + mo + dom + "_" + hod +  minit + sec
	}

	def isWellFormed (strIn: String): Boolean = {
		var bol: Boolean = false
		val q: String = if (strIn.count(_=='.')!=1)	{
			bol=true
			strIn.replace(".", "-").toString
		} else {
			bol=false
			strIn.toString
		}
		val r: String = if (q.count(_=='/')!=1)	{
			bol=true
			q.replace("/", "-").toString
		} else {
			bol=false
			q.toString
		}
		bol
	}

	def beWellFormed (strIn: String): String = {
		val q: String = if (strIn.count(_=='.')!=1)	{strIn.replace(".", "-").toString} else {strIn.toString}
		val r: String = if (strIn.count(_=='/')!=1)	{q.replace("/", "-").toString} else {q.toString}
		r.toString
	}

	def isISOdt (strIn: String): Boolean =
		try {
			val bwf = beWellFormed(strIn)
			val iwf: Boolean = isWellFormed(bwf)
			val r = strIn.replace(".","-")
			val s = r.replace("/","-")
			if (iwf)
			{df_ISO.parse(s)
				true}
			else
				false
		}		catch { case e: Exception => false
			// case e: java.time.format.DateTimeParseException => false  -- Java 18.
		}


	def isShortdt (strIn: String): Boolean =
		try {
			val bwf = beWellFormed(strIn)
			val iwf: Boolean = isWellFormed(bwf)
			val r = strIn.replace(".","-")
			val s = r.replace("/","-")
			if (iwf==true)
			{ df_Short.parse(s)
				//java.time.LocalDate.parse(s, df_Short)    -- Java 1.8
				true}
			else
				false
		}		catch { case e: Exception => false
			//case e: java.time.format.DateTimeParseException => false  -- Java 1.8
		}

	def isUSMinidt (strIn: String): Boolean =
		try {
			val bwf = beWellFormed(strIn)
			val iwf: Boolean = isWellFormed(bwf)
			val r = strIn.replace(".","-")
			val s = r.replace("/","-")
			if (iwf==true)
			{ df_USMini.parse(s)
				// java.time.LocalDate.parse(s, df_USMini)  -- Java 1.8
				true}
			else
				false
		}		catch { case e: Exception => false	}

	def isUSMini4dt (strIn: String): Boolean =
		try {
			val bwf = beWellFormed(strIn)
			val iwf: Boolean = isWellFormed(bwf)
			val r = strIn.replace(".", "-")
			val s = r.replace("/", "-")
			if (iwf == true) {
				df_USMini4.parse(s)
				// java.time.LocalDate.parse(s, df_USMini4)  -- Java 1.8
				true
			}
			else
				false
		}		catch { case e: Exception => false
			//case e: java.time.format.DateTimeParseException => false  -- Java 1.8
				}

	def isANSIdt (strIn: String): Boolean =
		try {
			val bwf = beWellFormed(strIn)
			val iwf: Boolean = isWellFormed(bwf)
			val r = strIn.replace(".", "-")
			val s = r.replace("/", "-")
			if (iwf == true) {
				df_ANSI.parse(s)
				//java.time.LocalDate.parse(s, df_ANSI)   -- Java 1.8
				true
			}
			else
				false
		}		catch { case e: Exception => false
			//case e: java.time.format.DateTimeParseException => false  -- Java 1.8
		}

	def isEUShortdt (strIn: String): Boolean =
		try {
			val bwf = beWellFormed(strIn)
			val iwf: Boolean = isWellFormed(bwf)
			val r = strIn.replace(".", "-")
			val s = r.replace("/", "-")
			if (iwf == true) {
				df_EUShort.parse(s)
				//java.time.LocalDate.parse(s, df_EUShort) -- Java 1.8
				true
			}
			else
				false
		}		catch { case e: Exception => false
			// case e: java.time.format.DateTimeParseException => false  -- Java 1.8
		}

	def isEULong (strIn: String): Boolean =
		try {
			val bwf = beWellFormed(strIn)
			val iwf: Boolean = isWellFormed(bwf)
			val r = strIn.replace(".", "-")
			val s = r.replace("/", "-")
			if (iwf == true) {
				df_EULong.parse(s)
				//java.time.LocalDate.parse(s, df_EULong)  -- Java 1.8
				true
			}
			else
				false
		}		catch { case e: Exception => false
			//case e: java.time.format.DateTimeParseException => false   -- Java 1.8
		}

	def isDate (strIn: String): Boolean =
		try {
			isISOdt(strIn)||isShortdt(strIn)||isUSMinidt(strIn)||isUSMini4dt(strIn)||isANSIdt(strIn)||isEUShortdt(strIn)||isEULong(strIn)
		}	catch { case e: Exception => false
			//case e: java.time.format.DateTimeParseException => false  -- Java 1.8
		}


}		// end of class


	
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
   def toNumeric : String = {
      if ((allCatch.opt(s.toDouble)).isDefined == true)
	     return s
      else
         return ("0")
    }		 

// replace a quoted string with commas with unquoted string with underscores
  def unQuote : String =
  {	
//     var quoteSeen  = false
//	 var t = new Array[Char](s.length)
	 
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


object AnonCharsDigits {
//  
// This function takes a string and generates a similar looking anonymized string
//
import java.security.MessageDigest

  val dtOp = new cs_DateOps	
	  
  def makeAnonStr(Str : String) : String = 
  {
   var AoC = new Array[Char](Str.length)
   var md5AoB = new Array[Byte](16)
   var substrEnd : Int = 0
   var myInt : Int = 0
   var c : Char = ' '
   
// return date fields unmodified

   if (dtOp.isDate(Str)) return Str
   
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

		  

object Anonymizer {

   val MainLog = LogManager.getRootLogger
   val AnonLog = LogManager.getLogger("AnonLogger")
   
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//   val fileOp = new cs_purlFileOp

//implicit def stringToString(s: String) = new StringImprovements(s)   
	  
   val conf = new SparkConf().setAppName("Anonymizer")
   val sc = new SparkContext(conf)  	  
	  

   def makeSubstData(inputFileName : String, outputFileName : String) : Int =
   { 
   // Read from the input file an array of alfanumeric labels and numbers, compute the mean and variance of numbers and then
   // generate a new array of normally distributed random numbers
   // Compute the mean and variance (for verification) and write a .csv file with the generated data
   // Anonymize alfanumeric labels

import org.apache.spark.mllib.linalg.DenseMatrix
import java.util.Random

   // read inputFile to a vector
   // inputFile will be RDD[String]
   
   var inpFile = sc.textFile(inputFileName)

   var numRows = inpFile.count().toInt

   AnonLog.info("Read " + inputFileName + ", Read " + numRows + " Rows")

   AnonLog.trace("print out a few rows read from file")
   inpFile.take(5).foreach(AnonLog.trace)
   
   
//////////////////////////////////////////////////////////////////////////////////////////
// Rectangularize the RDD, if needed before vectorizing
//

//implicit def stringToString(s: String) = new StringImprovements(s)

// filter element to remove quotes to prevent (quote) embedded commas
  val inpFileUq = inpFile.map(s => unQuote.removeEmbeddedCommas(s) )
//   inpFileUq.take(5).foreach(AnonLog.trace)

  val countFields = inpFileUq.map(s => s.split(',').length).collect()
  var RectangularizationNeeded = false
  
  AnonLog.info("number of fields in each row: ")

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
   outStr += "]" ; AnonLog.trace(outStr)   
  
  if (RectangularizationNeeded == true)
	 AnonLog.info("Identified jagged data set; Rectangularization needed")
  else
	 AnonLog.info("Identified rectangular data set; No fixup needed")

// infer number of columns by picking the largest of the top few

  AnonLog.info("Inferring longest row(s) has " + maxCount + " fields at row " + maxCountAt)
  
// rectangularize the RDD to numRows X maxCount

//   if (RectangularizationNeeded == true)
//   {	 
      var inpFileRe = inpFileUq.map(s => s + ",No Data")
      // remove short rows
      val shortFile = inpFileRe.filter(row => row.split(',').length < maxCount+1)
      AnonLog.trace("Short rows will be filtered out")		
      shortFile.take(40).foreach(AnonLog.trace)		  
      // truncate to maxCount+1 columns
      val inpFileTr = inpFileRe.filter(row => row.split(',').length == maxCount + 1)
//   }
//   else
//   {
      // dummy for compilation
//      val inpFileTr =   inpFileUq 
//   }	  

   // remove a header row   
//   if (RectangularizationNeeded == true)
//   {   
      var header = inpFileTr.first()              // header is a String
//   }
//   else
//   {
//      var header = inpFileUq.first()
//   }	  
   

   // split header into array of column header strings

   var hA = header.split(',')                 // hA is an Array of Strings

//   if (RectangularizationNeeded == true)
//   {      
      var inpFileNh = inpFileTr.filter(row => row != header)
//   }
//   else
//   {
//      var inpFileNh = inpFileUq.filter(row => row != header)
//   }	  
   

   AnonLog.trace("Removed the First row as Header")
   numRows = inpFileNh.count().toInt
   AnonLog.trace("number of rows = " + numRows)   

/////////////////////////////////////////////////////////////////////////////////////////   
   
import org.apache.spark.mllib.linalg.{Vector, Vectors}

  // parsedData will be org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vector]

   val parsedData = inpFileNh.map(s => Vectors.dense(s.split(',').map(replaceText.with0Str(_)).map(_.toDouble)))


   AnonLog.trace("print out a few vectors after converting from strings")
   parsedData.take(5).foreach(AnonLog.trace)   

import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

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
   
   var stdDev = new Array[Double](numCols)

   for (j <- 0 to numCols - 1)
      stdDev(j) = Math.sqrt(summary.variance(j))   

/*******************************************************************************
   // Identify possibly outlier data using Six Sigma filter
   // Display both row major (i.e. outlier items) and column major (outlier measurement process)
   // print out outliers 
 
   AnonLog.info("Identifying possibly outlier data using Six Sigma filter")
   AnonLog.info("Displaying both row major (outlier items) and column major (outlier processes)")

   outStr = "["   

   AnonLog.info("Column headers")
   
   for (j <- 0 to numCols -1)
   {
	  if (j != 0) outStr += ","
	  outStr = outStr + hA(j).toString   
   }
   outStr += "]" ; AnonLog.info(outStr)   


   // print out origDataFile rows whenever ANY field is outside [-3*stdDev, +3*stdDev]

   AnonLog.info("Detecting outlier data")
   
   val pdArr = parsedData.collect() // pdArr will be a 2-D Array of Double
   
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
	  //print(s"$j , "); println( aCol(numRows/2))
	  medians(j) = aCol(numRows/2)
	  if (j != 0) outStr += ","
	  outStr = outStr + medians(j).toString
   }
   outStr += "]" ; AnonLog.info("medians");  AnonLog.info(outStr)
   
   var j = 0
   var found = false
   var k = 0
   var kk = 0

   var param = 0.0
   var lowlim = 0.0
   var uplim = 0.0
   var deviation = 0.0

   // detect problem rows

   AnonLog.info("Row major(item) scan")

   for ( i <- 0 to numRows - 1)
   {
      k = 0
      for (j <- 0 to numCols -1 )
      {
         param = pdArr(i)(j)
	     lowlim = summary.mean(j) - 3.0*stdDev(j)
	     uplim = summary.mean(j) + 3.0*stdDev(j)
	  
         if ((param < lowlim) || (param > uplim))
	     {
	         // print(s"$i $j: ")
	         // for (kk <- 0 to k) print("  ")
		     // find deviation in sigmas
             deviation = (param - summary.mean(j))/stdDev(j)		  
    	     // println(s"$param [ $lowlim to $uplim ]   off by $deviation Sigmas" )
			 AnonLog.warn("( " + i + " , " + j + " ): " + param + " [ " + lowlim + " to " + uplim + " ] " + " off by " + deviation + " Sigmas")
		     k = k + 1
	     }
      }
   }

   // detect problem columns

   AnonLog.info("Column major(process) scan")

   for ( j <- 0 to numCols - 1)
   {
      k = 0
      for ( i <- 0 to numRows -1 )
      {
         param = pdArr(i)(j)
	     lowlim = summary.mean(j) - 3.0*stdDev(j)
	     uplim = summary.mean(j) + 3.0*stdDev(j)
	  
         if ((param < lowlim) || (param > uplim))
	     {
		     // find deviation in sigmas
             deviation = (param - summary.mean(j))/stdDev(j)		  
 			 AnonLog.warn("( " + i + " , " + j + " ): " + param + " [ " + lowlim + " to " + uplim + " ] " + " off by " + deviation + " Sigmas")
		     k = k + 1
	     }
  
      }
   }

*******************************************************************************/


import Array._

   var means = summary.mean.toArray
   var variances = summary.variance.toArray
   var normL1s = summary.normL1.toArray
   var normL2s = summary.normL2.toArray
   var maxs = summary.max.toArray
   var mins = summary.min.toArray

   var typeStrings = new Array[String](numCols)

   // infer columns where normL1, normL2, mean, variance, max and mean are 0 as non-numeric

   AnonLog.info("Inferring column data types")
   
   outStr = "["
   
   for (j <- 0 to numCols -1)
   {
      if ((normL1s(j) == 0) && (normL2s(j) == 0) && (means(j) == 0) && (variances(j) == 0) && (maxs(j) == 0) && (mins(j) == 0))
      {
         typeStrings(j) = "String"
      }	  
      else
      {
	     if ((normL1s(j).toInt.toDouble == normL1s(j)) && (maxs(j).toInt.toDouble == maxs(j)) && (mins(j).toInt.toDouble == mins(j)) )
	     {
	        typeStrings(j) = "Int"
         }		 
         else
	     {
            typeStrings(j) = "Double"	  
         }
      }
	  if (j != 0) outStr += ","
	  outStr = outStr + typeStrings(j)	  
   }
   
   outStr += "]" ; AnonLog.info(outStr)      

   var arStrings = ofDim[String](numRows, numCols)

   val parsedStringData = inpFileNh.flatMap(s => s.split(','))   // parsedStringData is an RDD
   
   //println();println("print out a few parsed string data");println()
   // parsedStringData.take(5).foreach(println); println()

   val pSA = parsedStringData.collect()                        // pSA is an Array of String

   for (i <- 0 to numRows -1)
   {
     for (j <- 0 to numCols -1)
     {
        arStrings(i)(j) = pSA(numCols*i + j)                   // fold pSA() to arStrings()()
		// if (j == 0) { AnonLog.trace( arStrings(i)(j) ) }
		// if (j == numCols - 1) AnonLog.trace( arStrings(i)(j) )
     }
   }  
	 
	 
  
   val rng = new Random

   AnonLog.trace("DenseMatrix randn( " + numRows + " , " + numCols + ")" )
   var dmrandn = DenseMatrix randn(numRows, numCols, rng)

   AnonLog.trace("print out a few rows generated....")

   for (i <- 0 to math.min(4, numRows - 1))
   {
     outStr = "["   
     for (j <- 0 to math.max(0, numCols - 1))
     {
       if (j != 0) outStr += ","	 
       if (typeStrings(j) == "String")
	   {
		   // Anonymize input to output
		   // AnonLog.trace(arStrings(i)(j))
		   // AnonLog.trace( AnonCharsDigits.makeAnonStr(arStrings(i)(j)))
	       outStr = outStr + AnonCharsDigits.makeAnonStr(arStrings(i)(j))		   
       }
       else
       {
          if (typeStrings(j) == "Int")
          {	   
			 // AnonLog.trace(dmrandn(i,j).toInt)
			 outStr = outStr + dmrandn(i,j).toInt.toString
          }
	      else
	      {
			 // AnonLog.trace(dmrandn(i,j))
			 outStr = outStr + dmrandn(i,j).toString
          }
       }
     }
     outStr += "]" ; AnonLog.info(outStr)   	 
   }	  
	  
   AnonLog.info("using means and variances of:")

   AnonLog.info("means:")
   
   outStr = "["
   
   for (j <- 0 to numCols - 1)
   {
      if (j != 0) outStr += ","   
      outStr = outStr + means(j).toString
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

   for (j <- 0 to numCols - 1)
      stdDev(j) = Math.sqrt(variances(j))
   
   var substData = ofDim[Double](numRows, numCols)

   def shiftAndScaleMat(maxRow:Int, maxCol:Int)
   {

       for (j <- 0 to maxCol - 1)
	     for (i <- 0 to maxRow - 1)
	     {
	         substData(i)(j) = means(j) + stdDev(j)*dmrandn(i,j)
         }		  
   }

   shiftAndScaleMat(numRows, numCols)

   AnonLog.trace("print out a few rows of transformed data")

   for (i <- 0 to math.min(4, numRows - 1))
   {
     outStr = "["
     for (j <- 0 to math.max(0, numCols - 1))
     {
       if (j != 0) outStr += ","	 
       if (typeStrings(j) == "String")
	   {
		  // Anonymize input to output
		  //AnonLog.trace(arStrings(i)(j))
		  // AnonLog.trace( AnonCharsDigits.makeAnonStr(arStrings(i)(j)))
	       outStr = outStr + AnonCharsDigits.makeAnonStr(arStrings(i)(j))		  
       }
       else
	   {
          if (typeStrings(j) == "Int")
          {	   
			 //AnonLog.trace(substData(i)(j).toInt)
			 outStr = outStr + substData(i)(j).toInt.toString			 
          }
	      else
	      {	
			 //AnonLog.trace(substData(i)(j))
			 outStr = outStr + substData(i)(j).toString			 
	      }
       }	   
     }
     outStr += "]" ; AnonLog.info(outStr)
   }

/*******************************************************************************
//   This is for Single machine use only!
//   
import java.io.PrintWriter
   val fileHandle = new PrintWriter(outputFileName)

   if (numCols > 1)
   {
      for (j <- 0 to math.max(0 , numCols - 2))
      {
		  // Don't anonymize header
		  fileHandle.print( hA(j)); fileHandle.print(",")
	  }
   }
   fileHandle.println( hA(numCols - 1))
   

   for (i <- 0 to numRows - 1)
   {
     if (numCols > 1)
     {
        for (j <- 0 to math.max(0 , numCols - 2))
        {
           if (typeStrings(j) == "String")
	       {
		      // Anonymize input to output
		      // fileHandle.print(arStrings(i)(j)); fileHandle.print(",")
              fileHandle.print( AnonCharsDigits.makeAnonStr(arStrings(i)(j))); fileHandle.print(",")
           }
           else
           {
              if (typeStrings(j) == "Int")
		      {
	             fileHandle.print(String.valueOf(substData(i)(j).toInt))
                 fileHandle.print(",")
              }
              else
              {		   
	             fileHandle.print(String.valueOf(substData(i)(j)))
                 fileHandle.print(",")
              }			  
		   }
        }
     }	 

     if (typeStrings(numCols - 1) == "String")
     {
	    // Anonymize input to output
	    // fileHandle.println(arStrings(i)(numCols - 1))
		fileHandle.println( AnonCharsDigits.makeAnonStr(arStrings(i)(numCols -1)))
     }
     else
     {
       if (typeStrings(numCols - 1) == "Int")
       {	 
	      fileHandle.println(String.valueOf(substData(i)(numCols - 1).toInt))
       }
       else
	   {
          fileHandle.println(String.valueOf(substData(i)(numCols - 1)))
       }
     }		
   }

   fileHandle.close()

*******************************************************************************/
/******************************************************************************/
//
// On hdfs, convert data back into RDD and write using saveAsTextFile(path)

   var outSA = new Array[String](numRows+1)
   // Don't anonymize Header
   outSA(0) = ""
   if (numCols > 1)
   {
      for (j <- 0 to math.max(0 , numCols - 2))
      {
		  // Don't anonymize header
		  outSA(0) += hA(j)
          outSA(0) += ","
	  }
   }
   outSA(0) += hA(numCols - 1)
   //outSA(0) += "\n"
   

   for (i <- 0 to numRows - 1)
   {
     outSA(i+1) = ""
     if (numCols > 1)
     {
        for (j <- 0 to math.max(0 , numCols - 2))
        {
           if (typeStrings(j) == "String")
	       {
		      // Anonymize input to output
              outSA(i+1) += AnonCharsDigits.makeAnonStr(arStrings(i)(j))
              outSA(i+1) += ","
           }
           else
           {
              if (typeStrings(j) == "Int")
		      {
	             //fileHandle.print(String.valueOf(substData(i)(j).toInt))
				 outSA(i+1) += String.valueOf(substData(i)(j).toInt)
                 outSA(i+1) += ","
              }
              else
              {		   
	             //fileHandle.print(String.valueOf(substData(i)(j)))
				 outSA(i+1) += String.valueOf(substData(i)(j))
                 outSA(i+1) += ","
              }			  
		   }
        }
     }	 

     if (typeStrings(numCols - 1) == "String")
     {
	    // Anonymize input to output
		//fileHandle.println( AnonCharsDigits.makeAnonStr(arStrings(i)(numCols - 1)))
        outSA(i+1) += AnonCharsDigits.makeAnonStr(arStrings(i)(numCols - 1))
        //outSA(i+1) += "\n"	
     }
     else
     {
       if (typeStrings(numCols - 1) == "Int")
       {	 
	      //fileHandle.println(String.valueOf(substData(i)(numCols - 1).toInt))
          outSA(i+1) += String.valueOf(substData(i)(numCols - 1).toInt)
          //outSA(i+1) += "\n"		  
       }
       else
	   {
          //fileHandle.println(String.valueOf(substData(i)(numCols - 1)))
          outSA(i+1) += String.valueOf(substData(i)(numCols - 1))
          //outSA(i+1) += "\n"		  
       }
     }		
   }

   val outpFile = sc.parallelize(outSA)
   
   outpFile.repartition(1).saveAsTextFile(outputFileName)   // repartition(1) is coalesce(1,true)
   
   AnonLog.info("Wrote directory " + outputFileName)

   // read outputFile to a vector

   // outputFile will be RDD[String]

   var substDataFile = sc.textFile(outputFileName + "/part-00000")

   AnonLog.trace("Read " + outputFileName + "/part-00000")

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

   } // makeSubstData()

   
  var rc = -1
  
  def main(args : Array[String]) = {

// Log level hierarchy		  
// WARN includes ERROR and FATAL
// ALL includes TRACE, DEBUG, INFO, WARN, ERROR, FATAL
// use OFF to turn everything off
//

import java.io.IOException

    try
	{
	
      MainLog.setLevel(Level.WARN)
	  
      AnonLog.setLevel(Level.ALL)
	  
      AnonLog.info("Anonymizing using makeSubstData")

	  
	  if (args.length < 2)
	     AnonLog.error("Too few arguments; need input directory and output directory paths")
	  else
	     AnonLog.info("input = " + args(0) + " output = " + args(1))
		 
	  //rc = makeSubstData("hdfs:///appdata/ezone/cqn_mve_stg/staging/UnitLevelGroup.csv", "hdfs:///appdata/ezone/cqn_mve_stg/staging/UnitLevelGroupAnon")
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