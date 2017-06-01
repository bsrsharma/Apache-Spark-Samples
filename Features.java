import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import org.apache.spark.mllib.linalg.DenseMatrix;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.PrintWriter;

import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.KolmogorovSmirnovTestResult;

import java.io.IOException;

import java.util.Arrays;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import scala.Tuple2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.stat.KernelDensity;


public final class Features
{
   // replace quote with space and embedded comma with underscore
   // as these will cause errors in converting a .csv to RDD
   static class unquoteRemoveEmbeddedCommas implements Function<String, String>   
   {
      @Override
      public String call(String s)
      {
         boolean quoteSeen  = false;
         char[] t = new char[s.length()];
         int i;
	 
         for (i = 0; i < s.length(); i++)
         {
	    t[i] = s.charAt(i);
            if ((s.charAt(i) == '\'' ) || (s.charAt(i) == '"'))
            {
	       if (quoteSeen == true)
               {
                  quoteSeen = false;
               }
               else
               {
	          quoteSeen = true;
               }			
               t[i] = ' ';
            }
            if ((s.charAt(i) == ',') && (quoteSeen == true))
            {
	       t[i] = '_';
            }         	  
         }
	 
	 return String.valueOf(t);
      }	 
   }  // unquoteRemoveEmbeddedCommas
   
    
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
   
   static class Normalize implements Function<String[], String[]>
   {
      @Override
      public String[] call(String[] sAin)
      {
         String sAout[] = new String[sAin.length];
         int j;
         
         for (j = 0; j < sAin.length; j++)
         {
            if(typeStrings[j] == "String")
               sAout[j] = sAin[j];
            else
               try
               {
                  sAout[j] = String.valueOf( (Double.parseDouble(sAin[j]) - mean[j])/stdDev[j]);
               }
               catch (NumberFormatException nFE)
               {
                  sAout[j] = String.valueOf(mean[j]);
               }
         }
         return sAout;
      }
   }
   
   static class NormL1L2 implements Function<String[], String[]>
   {
      @Override
      public String[] call(String[] sAin)
      {
         String sAout[] = new String[3];
         int j;
         double normL1, normL2Sq;

         // sAin[0] is assumed to be ID info.; else add other ID info here
         sAout[0] = sAin[0];
         normL1 = 0.0;
         normL2Sq = 0.0;
         for (j = 1; j < sAin.length; j++)
         {
            if(typeStrings[j] != "String")
            {
               normL1 += Math.abs((Double.parseDouble(sAin[j])));
               normL2Sq += Math.pow((Double.parseDouble(sAin[j])), 2.0);
            }               
         }
         sAout[1] = String.valueOf(normL1);
         sAout[2] = String.valueOf(Math.sqrt(normL2Sq));
         
         return sAout;
      }
   }
   
   static int sortKeyIndex;
   
   static class KeyNormL1L2 implements PairFunction<String[], Double, String[]>
   {
      @Override
      public Tuple2<Double, String[]> call(String[] sAin)
      {
         Tuple2<Double, String[]> t2DSA = new Tuple2<>(Double.parseDouble(sAin[sortKeyIndex]), sAin);
         
         return t2DSA;
      }
   }
   
   static class MarkColumn implements PairFunction<String[], Double, Integer>
   {
      @Override
      public Tuple2<Double, Integer> call(String[] sAin)
      {
         Tuple2<Double, Integer> t2DI = new Tuple2<>(Double.parseDouble(sAin[sortKeyIndex]), 1);
         
         return t2DI;
      }
   }
   
   static double mean[], stdDev[], colMin[];
   static String typeStrings[];
   static int maxCount = 0;
   static double binWidth;
   
   public static int findFeatures(JavaSparkContext sc, String inputFileName, String outputFileName, String normsFileName)
   { 
   // Read from the input file an array of alfanumeric labels and numbers, compute the mean and variance of numbers and then
   // generate a new array of normalized features
   // Compute the normL1 abd normL2 and write a .csv file with the generated data
   // write out a .csv file of normalized data
   

      int rc = 0;
      int i,j;
      int numRows;
      int numCols;
      
      MultivariateStatisticalSummary summary;
      
      JavaRDD<String> inpFile = sc.textFile(inputFileName);
      
      numRows = (int)inpFile.count();

      System.out.println("Read " + inputFileName + ", Read " + numRows + " Rows");
      
      numCols = inpFile.first().split(",").length;
      
      System.out.println("print out a few rows read from file");
      System.out.println( inpFile.take(5));      
      
      // filter element to remove quotes to prevent (quote) embedded commas
      JavaRDD<String> inpFileUq = inpFile.map(new unquoteRemoveEmbeddedCommas());
      
      Integer countFields[] = new Integer[numRows];
      countFields = inpFileUq.map(s -> s.split(",").length).collect().toArray(countFields);
      
      boolean RectangularizationNeeded = false;
  
      System.out.println("Compute number of fields in each row: ");

      int maxCountAt = 0;
  
      for (i = 0; i < numRows; i++)
      {
         if (countFields[i] > maxCount)
	 {
	    maxCount = countFields[i]; maxCountAt = i;
         }
	
         if ((RectangularizationNeeded == false) && (i > 0))
	 {
	    if (countFields[i] != countFields[i-1])
	    {
	       RectangularizationNeeded = true;
	    }
         }	   
      }	
  
      if (RectangularizationNeeded == true)
	 System.out.println("Identified jagged data set; Rectangularization needed");
      else
	 System.out.println("Identified rectangular data set; No fixup needed");

      System.out.println("Inferring longest row(s) has " + maxCount + " fields at row " + maxCountAt);
  
      // rectangularize the RDD to numRows X maxCount
      
      String header = inpFileUq.first();
      System.out.println("header: " + header);

      JavaRDD<String> inpFileNh = inpFileUq.filter(row -> (row.equals(header) == false));
  
      System.out.println("Removed the First row as Header");
      

      JavaRDD<String> inpFileRe = inpFileNh.map(s -> s + ",No Data");
      // remove short rows
      JavaRDD<String> shortFile = inpFileRe.filter(row -> row.split(",").length < maxCount+1);
      System.out.println("Short rows will be filtered out");
      System.out.println(shortFile.take(40));		  
      // truncate to maxCount+1 columns
      JavaRDD<String> inpFileTr = inpFileRe.filter(row -> row.split(",").length == maxCount + 1);
      numRows = (int)inpFileTr.count();
      System.out.println("number of rows = " + numRows);
     
      numCols = inpFileTr.first().split(",").length;
      
      System.out.println("print out a few rows");
      System.out.println( inpFileTr.take(5));
      
      JavaRDD<String[]> inpFileSA = inpFileTr.map(s -> s.split(",")).cache();
      
      JavaRDD<Vector> parsedInpV = inpFileSA.map(new ParseDouble()).map(dA -> Vectors.dense(dA));
      
      summary = Statistics.colStats(parsedInpV.rdd());
      
      System.out.println("print out summary statistics, mean and variance, for each column");
      System.out.println(summary.mean());
      System.out.println(summary.variance());

      System.out.println("summary.count"); System.out.println(summary.count());
      System.out.println("summary.max"); System.out.println(summary.max());
      System.out.println("summary.min"); System.out.println(summary.min());
      System.out.println("summary.normL1"); System.out.println(summary.normL1());
      System.out.println("summary.normL2"); System.out.println(summary.normL2());
      System.out.println("summary.numnonZeros"); System.out.println(summary.numNonzeros());
      
      
      mean = new double[numCols];
      double variance[] = new double[numCols];
      stdDev = new double[numCols];
      colMin = new double[numCols];
      
      for (j = 0; j < numCols; j++)
      {
         mean[j] = summary.mean().apply(j);
         variance[j] = summary.variance().apply(j);
         stdDev[j] = Math.sqrt(variance[j]);
         colMin[j] = summary.min().apply(j);
      }
      
      typeStrings = new String[numCols];
      
      // infer columns where normL1, normL2, mean, variance, max and mean are 0 as non-numeric

      System.out.println("Inferring column data types");
   
      for (j = 0; j < numCols; j++)
      {
         if ((summary.normL1().apply(j) == 0) && (summary.normL2().apply(j) == 0) && (summary.mean().apply(j) == 0) && (summary.variance().apply(j) == 0) &&
             (summary.max().apply(j) == 0) && (summary.min().apply(j) == 0))
         {
            typeStrings[j] = "String";
         }	  
         else
         {
	    if ((summary.normL1().apply(j) == (double)(int)summary.normL1().apply(j)) && (summary.max().apply(j) == (double)(int)summary.max().apply(j)) && 
	        (summary.min().apply(j) == (double)(int)summary.min().apply(j)) )
	    {
	       typeStrings[j] = "Int";
            }		 
            else
	    {
               typeStrings[j] = "Double";
            }
         }
      }
      
      for (String s: typeStrings)
         System.out.print( s + "\t" );
      System.out.println();
      
      
/******************************************************************************/ 

   Matrix correlMatrix = Statistics.corr(parsedInpV.rdd(), "pearson");
   
   System.out.println("Computing Correlation Matrix on all columns");
   System.out.println("Printing out column positions that have correlation coefficient > 0.5 or < -0.5");

   for (i = 0; i < numCols; i++)
      for (j = 0; j < i; j++)
	     if (((correlMatrix.apply(i, j) >= 0.5) || (correlMatrix.apply(i, j) <= -0.5)) && (i != j) && (typeStrings[i] != "String") && (typeStrings[j] != "String"))
		    System.out.println(i + " , " + j + " , " + correlMatrix.apply(i, j));
			
/******************************************************************************/
      
      JavaRDD<String[]> outData = inpFileSA.map(new Normalize());

      JavaRDD<String[]> normL1L2 = outData.map(new NormL1L2());
      
      sortKeyIndex = 1;
      //JavaPairRDD<Double, String[]> keyL1NormL1L2 = normL1L2.mapToPair(new KeyNormL1L2()).sortByKey();
      
      System.out.println("outData:");
      for (String[] sA: outData.take(5))
      {
         for (String s: sA)
            System.out.print( s + "\t" );
         System.out.println();
      }
      
      System.out.println("normL1L2:");
      for (String[] sA: normL1L2.take(5))
      {
         for (String s: sA)
            System.out.print( s + "\t" );
         System.out.println();
      }
      
      System.out.println("key L1: NormL1L2:");
      for (Tuple2<Double, String[]> tDSA: normL1L2.mapToPair(new KeyNormL1L2()).sortByKey().take(5))
      {
         System.out.println( tDSA._2()[0] + "\t" + tDSA._2()[1] + "\t" + tDSA._2()[2]);
      }
      
      System.out.println("Descending: key L1: NormL1L2:");
      for (Tuple2<Double, String[]> tDSA: normL1L2.mapToPair(new KeyNormL1L2()).sortByKey(false).take(5))
      {
         System.out.println( tDSA._2()[0] + "\t" + tDSA._2()[1] + "\t" + tDSA._2()[2]);
      }
      
      sortKeyIndex = 2;
      System.out.println("key L2: NormL1L2:");

      for (Tuple2<Double, String[]> tDSA: normL1L2.mapToPair(new KeyNormL1L2()).sortByKey().take(5))
      {
         System.out.println( tDSA._2()[0] + "\t" + tDSA._2()[1] + "\t" + tDSA._2()[2]);
      }
      
      System.out.println("Descending: key L2: NormL1L2:");
      for (Tuple2<Double, String[]> tDSA: normL1L2.mapToPair(new KeyNormL1L2()).sortByKey(false).take(5))
      {
         System.out.println( tDSA._2()[0] + "\t" + tDSA._2()[1] + "\t" + tDSA._2()[2]);
      }
      
      sortKeyIndex = 1;
      JavaDoubleRDD col1 = sc.parallelizeDoubles(inpFileSA.map(sA -> Double.parseDouble(sA[sortKeyIndex])).collect());      
      
      KolmogorovSmirnovTestResult testResult = Statistics.kolmogorovSmirnovTest(col1, "norm", mean[1], stdDev[1]);
      // summary of the test including the p-value, test statistic, and null hypothesis
      // if our p-value indicates significance, we can reject the null hypothesis
      System.out.println("Column[ " + sortKeyIndex + " ]\n" + testResult);
      
      sortKeyIndex = 3;
      JavaRDD<Double> col3 = inpFileSA.map(sA -> Double.parseDouble(sA[sortKeyIndex]));
      
      KernelDensity kd = new KernelDensity().setSample(col3).setBandwidth(3.0);

      // Find density estimates for the given values
      int sAS = (int)Math.sqrt(numRows);                                   // sample array size
      double samplePoints[] = new double[sAS];
      for (i = 0; i < sAS; i++)
         samplePoints[i] = i*sAS;

      double[] densities = kd.estimate(samplePoints);

      System.out.println("Kernel densities for column [ " + sortKeyIndex + " ]: " + Arrays.toString(densities));
      
      /*******************************************************************************/
      //
      //  compute Skewness and Kurtosis for a column
      //
      sortKeyIndex = 5;
      JavaRDD<Double> col5 = inpFileSA.map(sA -> Double.parseDouble(sA[sortKeyIndex]));
      
      double sumOfPow3 = col5.map(d -> Math.pow(d, 3.0)).reduce((a, b) -> a + b);
      double sumOfPow4 = col5.map(d -> Math.pow(d, 4.0)).reduce((a, b) -> a + b);
      
      double[] skew = new double[numCols];
      double[] kurt = new double[numCols];
      
      skew[sortKeyIndex] = sumOfPow3/numRows;
      kurt[sortKeyIndex] = (sumOfPow4/numRows) - 3.0;
      
      System.out.println("skew[" + sortKeyIndex + "] = " + skew[sortKeyIndex] + "; kurtosis[" + sortKeyIndex+"] = " + kurt[sortKeyIndex]);
      
      // compute median for a column
      
      sortKeyIndex = 3;
      //JavaPairRDD<Double, String[]> col3Key = inpFileSA.mapToPair(new KeyNormL1L2()).sortByKey();
      Tuple2<Double, String[]> col3Median = (inpFileSA.mapToPair(new KeyNormL1L2()).sortByKey().take((int)(numRows/2))).get((int)(numRows/2) - 1);
      System.out.println("median[" + sortKeyIndex + "] = " + col3Median._1());
      
      // compute Histogram for a column
      
      sortKeyIndex = 1;
      int numBins = (int)Math.sqrt(numRows);
      binWidth = (summary.max().apply(sortKeyIndex) - colMin[sortKeyIndex])/numBins;
      
      JavaPairRDD<Integer, Integer> colMark = inpFileSA.mapToPair(new MarkColumn()).mapToPair(KV -> new Tuple2<>((int)((KV._1() - colMin[sortKeyIndex])/binWidth), 1) );
      
      JavaPairRDD<Double, Integer> histogram = colMark.reduceByKey((a,b) -> a + b).sortByKey().mapToPair(KV -> new Tuple2<>(KV._1()*binWidth + colMin[sortKeyIndex], KV._2()) );
      System.out.println("Histogram[" + sortKeyIndex + "]:\n" + histogram.take(numBins));
      
      // sort by modes
      JavaPairRDD<Double, Integer> modes = histogram.mapToPair(KV -> new Tuple2<>(KV._2(), KV._1()) ).sortByKey(false).mapToPair(KV -> new Tuple2<>(KV._2(), KV._1()) );
      System.out.println("Modes[" + sortKeyIndex + "]:\n" + modes.take(numBins));
      
      inpFileSA.unpersist();
      
      numCols--; // remove "No Data"
      
      try
      {
         PrintWriter fileHandle = new PrintWriter(outputFileName);
         
         fileHandle.println(header);
         
         for (String[] sA: outData.take(numRows))
         {
            j = 0;
            for (String s: sA)
            {
               if (j < numCols - 1)
               {
                  fileHandle.print(s);
                  fileHandle.print(",");
               }
               if (j == numCols - 1)
                  fileHandle.println(s);
               j++;
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

      JavaRDD<double[]> parsedDataD = parsedData.map(new ParseDouble());

      JavaRDD<Vector> parsedDataV = parsedDataD.map(dA -> Vectors.dense(dA));
      
      System.out.println( parsedDataV.take(5) );
      
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

   } // findFeatures()

   
   public static void main(String[] args)
   {
   
         SparkSession spark = SparkSession.builder().appName("Features").getOrCreate();

         JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
         
         findFeatures(sc, "/home/bsrsharma/work/java/arran.csv", "/home/bsrsharma/work/java/normalized.csv", "/home/bsrsharma/work/java/L1L2norms.csv");

         System.out.println("Features finally ended");

         spark.stop();
         
   }  // main
}  // Features