
// This function returns a "0" if the operand string can't be converted to a number; otherwise, the input is unaltered
import scala.util.control.Exception._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseMatrix
import java.util.Random
import Array._
import java.io.PrintWriter
import java.io.IOException
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

object replaceText  {
   def with0Str(s : String) : String = {
      if ((allCatch.opt(s.toDouble)).isDefined == true)
         return s
      else
         return ("0")
    }
}  // replaceText 

object makeNiceStr {

   def makeNiceStr(AoB : Array[Byte]) : String = 
   {
      val niceAoC = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

      var AoC = new Array[Char](AoB.length)
      var myInt : Int = 0
      var c : Char = ' '
   
      for (i <- 0 to AoB.length - 1)
      {
         // AoB(i) = AoB(i) & 31
         // workaround for Scala bug
         myInt = AoB(i)
         myInt = myInt & 31
         // prevent first char numeric (to prevent all numeric labels!)	  
	 if ((i == 0) && (myInt > 23))
	 {
	    myInt = myInt - 24
         }
         c = niceAoC.charAt(myInt)
	 AoC(i) = c
      }
      return(AoC.mkString(""))
    }
}

object makeAlfaNumData
{
   val conf = new SparkConf().setAppName("makeAlfaNumData")
   val sc = new SparkContext(conf)  

   def fnMakeAlfaNumData(outputFileName : String, numRows : Int, numCols : Int, meanAndVarianceArrs : Double* )
   { 
   // Generate an array of normally distributed random numbers
   // Compute their statistics and write a .csv file with the generated data

      var meanAndVariancesArrLength = meanAndVarianceArrs.length
      var numColsTimes2 = 2*numCols

      if (meanAndVariancesArrLength != 2*numCols)
      {
         println("Input Error, number of mean and variances incorrect; should match numCols")
         println(s"numCols = $numCols, number of mean and variances = $meanAndVariancesArrLength (both put together); Should be $numColsTimes2" )
         println("Usage: makeData(outputFileName : String, numRows : Int, numCols : Int, meanAndVarianceArrs : Double* )")
      
         return
      }

      val rng = new Random

      println(); println("print out a few rows generated...."); println();

      var vID = new Array[Byte](13)

      for (i <- 0 to math.min(4, numRows - 1))
      {
         println()
         // print a 13 char visual ID
         scala.util.Random.nextBytes(vID)
         print(makeNiceStr.makeNiceStr(vID)); print("\t")  
         for (j <- 0 to math.max(0, numCols - 1))
         {
            print(scala.util.Random.nextGaussian())
	    print("\t")
	    scala.util.Random.nextBytes(vID)
            print(makeNiceStr.makeNiceStr(vID.dropRight(scala.util.Random.nextInt(10)))); print("\t")	
         }
      }	  
	  
      println(); println(".....")

      println();println(); println("using means and variances of:")

      var mean = new Array[Double](numCols)
      var variance = new Array[Double](numCols)

      for (j <- 0 to numCols - 1)
      {
         mean(j) = meanAndVarianceArrs(j);
         variance(j) = meanAndVarianceArrs(numCols+j)
      }

      println();println("means:");
      for (j <- 0 to numCols - 1)
      {
         print(mean(j)); print("\t")
      }
      println()

      println();println("variances:")
      for (j <- 0 to numCols - 1)
      {
         print(variance(j)); print("\t")
      }
      println();
      println()
   
      var stdDev = new Array[Double](numCols)

      for (j <- 0 to numCols - 1)
         stdDev(j) = Math.sqrt(variance(j))

      println("print out a few rows of transformed data" ); println();

      for (i <- 0 to math.min(4, numRows - 1))
      {
         println()
         scala.util.Random.nextBytes(vID)
         print(makeNiceStr.makeNiceStr(vID)); print("\t")  
         for (j <- 0 to math.max(0, numCols - 1))
         {
            print(scala.util.Random.nextGaussian()*stdDev(j) + mean(j))
            print("\t")
            // Add alfa fields
	    scala.util.Random.nextBytes(vID)
            print(makeNiceStr.makeNiceStr(vID.drop(scala.util.Random.nextInt(10)))); print("\t")	 
         }
      }
      println(); println(".....")


      val fileHandle = new PrintWriter(outputFileName)

      for (i <- 0 to numRows - 1)
      {

         scala.util.Random.nextBytes(vID)
         fileHandle.print(makeNiceStr.makeNiceStr(vID))
         fileHandle.print(",")
         if (numCols > 1)
         {
            for (j <- 0 to math.max(0 , numCols - 2))
            {
	       fileHandle.print(scala.util.Random.nextGaussian()*stdDev(j) + mean(j))
               fileHandle.print(",")
               scala.util.Random.nextBytes(vID)
               fileHandle.print(makeNiceStr.makeNiceStr(vID).dropRight(scala.util.Random.nextInt(10))); fileHandle.print(",")
            }
            fileHandle.println(scala.util.Random.nextGaussian()*stdDev(numCols-1) + mean(numCols-1))
         }
      }

      fileHandle.close()

      println();println(s"Wrote $outputFileName"); println()

      // read outputFile to a vector

      // outputFile will be RDD[String]

      val arrnFile = sc.textFile(outputFileName)

      println();println(s"Read $outputFileName");println();

      println("print out a few rows read from file");println()
      arrnFile.take(5).foreach(println); println()

      // parsedData will be org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vector]

      val parsedData = arrnFile.map(s => Vectors.dense(s.split(',').map(replaceText.with0Str(_)).map(_.toDouble)))
      println();println("print out a few vectors after converting from strings");println()
      parsedData.take(5).foreach(println); println()

      val summary: MultivariateStatisticalSummary = Statistics.colStats(parsedData)

      println();println("print out summary statistics, mean and variance, for each column");println()
      println(summary.mean)
      println(summary.variance);println()

      println("summary.count"); println(summary.count); println()
      println("summary.max"); println(summary.max); println()
      println("summary.min"); println(summary.min); println()
      println("summary.normL1"); println(summary.normL1); println()
      println("summary.normL2"); println(summary.normL2); println()
      println("summary.numnonZeros"); println(summary.numNonzeros); println()

   } // fnMakeAlfaNumData()


      var rc = -1
  
      def main(args : Array[String]) = {

      try
      {     
         fnMakeAlfaNumData("/home/bsrsharma/work/scala/arran.csv", 10000,3, 1.2, 3.4, 5.6,  7.8, 9.10, 11.12)
      }
      catch
      {
         case ex: IOException  => println("makeAlfaNumData threw " + ex.toString)
      }
      finally
      {
         println("makeAlfaNumData finally ended")
      } 
   }  // main
}  // makeAlfaNumData