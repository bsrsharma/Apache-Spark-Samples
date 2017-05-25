import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import org.apache.spark.mllib.linalg.DenseMatrix;
import java.util.Random;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.PrintWriter;

import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

import java.io.IOException;

import java.util.Arrays;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function;
 

public final class MakeAlfaNumData
{

   public static String makeNiceStr (byte[] AoB) 
   {
      String niceAoC = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

      char[] AoC = new char[AoB.length];
      int i;
      int myInt;
      char c;
      
   
      for (i = 0; i < AoB.length; i++)
      {
         // AoB[i] = AoB[i] & 31
         myInt = AoB[i];
         myInt = myInt & 31;
         // prevent first char numeric (to prevent all numeric labels!)	  
	 if ((i == 0) && (myInt > 23))
	 {
	    myInt = myInt - 24;
         }
         c = niceAoC.charAt(myInt);
	 AoC[i] = c;
      }
      return(String.valueOf(AoC));
    }
    
   public static void shiftAndScaleMat (int maxRow, int maxCol, double mean[], double stdDev[], DenseMatrix origData, double arrn[][] )
   {
   
      int i,j;

      for (j = 0; j < maxCol; j++)
         for (i = 0; i < maxRow; i++)
	 {
	    arrn[i][j] = mean[j] + stdDev[j]*origData.apply(i, j);
         }		  
   }
   
   static class ParseDouble implements Function<String[], double[]>
   {
      @Override
      public double[] call(String[] sA)
      {
         double dA[] = new double[sA.length];
         int j;
         
         for (j = 0; j < sA.length; j++)
         try
         {
            dA[j] = Double.parseDouble(sA[j]);
         }
         catch (NumberFormatException nFE)
         {
            dA[j] = 0.0;
         }                   
            
         return dA;
      }
   }   


   public static int fnMakeAlfaNumData(JavaSparkContext sc, String outputFileName, int numRows, int numCols, double meanAndVarianceArrs[])
   { 
   // Generate an array of normally distributed random numbers
   // Compute their statistics and write a .csv file with the generated data

      int rc = 0;
      int meanAndVariancesArrLength;
      int numColsTimes2;
      int i,j;
      
      Random rng = new Random();
      
      byte vID[] = new byte[16];
      int rblen;
      double arrn[][] = new double[numRows][numCols];
      
      meanAndVariancesArrLength = meanAndVarianceArrs.length;
      numColsTimes2 = 2*numCols;

      if (meanAndVariancesArrLength != 2*numCols)
      {
         System.out.println("Input Error, number of mean and variances incorrect; should match numCols");
         System.out.println("numCols = " + numCols + "number of mean and variances = " + meanAndVariancesArrLength + " (both put together); Should be numColsTimes2" );
         System.out.println("Usage: makeData(outputFileName : String, numRows : Int, numCols : Int, meanAndVarianceArrs : Double* )");
         return -1;
      }

      System.out.println("DenseMatrix randn( " + numRows + " , " + numCols + " )");

      double values[] = new double[numRows*numCols];
      
      DenseMatrix origData = new DenseMatrix(numRows, numCols, values);
      origData = origData.randn(numRows, numCols, rng);

      System.out.println("print out a few rows generated....");

      for (i = 0; i < Math.min(4, numRows); i++)
      {
         // print a 13 char visual ID
         rng.nextBytes(vID);
         System.out.print(makeNiceStr(vID) + "\t");
         for (j = 0; j < numCols; j++)
         {
            System.out.print(origData.apply(i,j) + "\t");
	    rng.nextBytes(vID);
	    rblen = rng.nextInt(10);
            System.out.print(makeNiceStr(vID).substring(0,6+rblen) + "\t");
         }
         System.out.println();
      }	  
	  
      System.out.println("using means and variances of:");

      double mean[] = new double[numCols];
      double variance[] = new double[numCols];

      for (j = 0; j < numCols; j++)
      {
         mean[j] = meanAndVarianceArrs[j];
         variance[j] = meanAndVarianceArrs[numCols+j];
      }

      System.out.println("means:");
      for (j = 0; j < numCols; j++)
      {
         System.out.print(mean[j] + " , ");
      }
      System.out.println();

      System.out.println("variances:");
      for (j = 0; j < numCols; j++)
      {
         System.out.print(variance[j] + " , ");
      }
      System.out.println();
   
      double stdDev[] = new double[numCols];

      for (j = 0; j < numCols; j++)
         stdDev[j] = Math.sqrt(variance[j]);

      shiftAndScaleMat(numRows, numCols, mean, stdDev, origData, arrn);

      System.out.println("print out a few rows of transformed data" );

      for (i = 0; i < Math.min(4, numRows); i++)
      {
         rng.nextBytes(vID);
         System.out.print(makeNiceStr(vID) + "\t");
         for (j = 0; j < numCols; j++)
         {
            System.out.print(arrn[i][j] + "\t");
	    rng.nextBytes(vID);
	    rblen = rng.nextInt(10);
            System.out.print(makeNiceStr(vID).substring(0,6+rblen) + "\t");
         }
         System.out.println();
      }

      try
      {
         PrintWriter fileHandle = new PrintWriter(outputFileName);


         for (i = 0; i < numRows; i++)
         {
            rng.nextBytes(vID);
            fileHandle.print(makeNiceStr(vID));
            fileHandle.print(",");
            if (numCols > 1)
            {
               for (j = 0; j < numCols - 1; j++)
               {
	          fileHandle.print(String.valueOf(arrn[i][j]));
                  fileHandle.print(",");
                  rng.nextBytes(vID);
	          rblen = rng.nextInt(10);
                  fileHandle.print(makeNiceStr(vID).substring(0,6+rblen)); fileHandle.print(",");
               }
               fileHandle.println(String.valueOf(arrn[i][numCols - 1]));
            }
         }
         fileHandle.close();
      }
      
      catch   (java.io.FileNotFoundException FNFE)
      {
         System.out.println("PrintWriter threw " + FNFE);
      }               

      System.out.println("Wrote " + outputFileName);

      JavaRDD<String> arrnFile = sc.textFile(outputFileName);

      System.out.println("Read " + outputFileName);

      System.out.println("print out a few rows read from file");
      
      System.out.println( arrnFile.take(5) );

      JavaRDD<String[]> parsedData = arrnFile.map(s -> s.split(","));
      
      System.out.println("print out a few vectors after converting from strings");
/*      
      for (String[] sA: parsedData.take(5))
      {
         for (String s: sA)
            System.out.print( s + "\t" );
         System.out.println();
      }             
*/      
      JavaRDD<double[]> parsedDataD = parsedData.map(new ParseDouble());
/*      
      for (double[] dA: parsedDataD.take(5))
      {
         for (double d: dA)
            System.out.print( d + "\t" );
         System.out.println();
      }
      
      i = 0;
      for (double[] dA: parsedDataD.take(numRows))
      {
         if ((dA[0] != 0.0) || (dA[2] != 0.0) || (dA[4] != 0.0))
           System.out.println(i+"\t"+dA[0]+"\t"+dA[1]+"\t"+dA[2]+"\t"+dA[3]+"\t"+dA[4]+"\t"+dA[5]);
         i++;           
      }
*/      
      JavaRDD<Vector> parsedDataV = parsedDataD.map(dA -> Vectors.dense(dA));
      
      System.out.println( parsedDataV.take(5) );
      
      MultivariateStatisticalSummary summary;
      
      summary = Statistics.colStats(parsedDataV.rdd());
      
      System.out.println("print out summary statistics, mean and variance, for each column");
      System.out.println(summary.mean());
      System.out.println(summary.variance());

      System.out.println("summary.count"); System.out.println(summary.count());
      System.out.println("summary.max"); System.out.println(summary.max());
      System.out.println("summary.min"); System.out.println(summary.min());
      System.out.println("summary.normL1"); System.out.println(summary.normL1());
      System.out.println("summary.normL2"); System.out.println(summary.normL2());
      System.out.println("summary.numnonZeros"); System.out.println(summary.numNonzeros());
      
      return rc;

   } // fnMakeAlfaNumData()


   int rc = -1;
  
   
   public static void main(String[] args)
   {
   
         SparkSession spark = SparkSession.builder().appName("makeAlfaNumData").getOrCreate();

         JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
         
         double meanAndVarianceArrs[] = { 1.2, 3.4, 5.6,  7.8, 9.10, 11.12 };

         fnMakeAlfaNumData(sc, "/home/bsrsharma/work/java/arran.csv", 10000,3, meanAndVarianceArrs);

         System.out.println("makeAlfaNumData finally ended");

         spark.stop();
         
   }  // main
}  // MakeAlfaNumData