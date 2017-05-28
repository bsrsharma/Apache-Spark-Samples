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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.ArrayList;

public final class Anonymizer
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
   
// This function takes a string and generates a similar looking anonymized string
   public static String makeAnonStr(String Str) 
   {
      char[] AoC = new char[Str.length()];
      byte[] md5AoB = new byte[16];
      int substrEnd = 0;
      int myInt = 0;
      char c = ' ';
      int i;
   
// ToDo: return date fields as valid dates
   
      for (i = 0; i < Str.length(); i++)
      {
         if (i % 16 == 0)
         {
            substrEnd = i + 16;
            if (i + 16 > Str.length())
               substrEnd =  Str.length();
            try
            {
	       md5AoB = MessageDigest.getInstance("MD5").digest(Str.substring(i, substrEnd).getBytes());
            }
            catch (NoSuchAlgorithmException NSAE)
            {
	       System.out.println("Error in md5() - " + NSAE);
	       return "";
           }
         }
   
         c = Str.charAt(i);
	  
         AoC[i] = c;
         myInt = md5AoB[i % 16] + 128;
	  
         if (c >= 'A' && c <= 'Z')
	    AoC[i] = (char)((myInt % 26) + 'A');
		
         if (c >= 'a' && c <= 'z')
	    AoC[i] = (char)((myInt % 26) + 'a');

         if (c >= '0' && c <= '9')
	    AoC[i] = (char)((myInt % 10) + '0');
		
      }
      return String.valueOf(AoC);
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
   
   static double mean[], stdDev[];
   static String typeStrings[];
   static Random rng;
   static int maxCount = 0;
   
   static class Anon implements Function<String[], String[]>
   {
      @Override
      public String[] call(String[] sAin)
      {
         String sAout[] = new String[sAin.length];
         int j;
         
         for (j = 0; j < sAin.length; j++)
         {
            if(typeStrings[j] == "String")
               sAout[j] = makeAnonStr(sAin[j]);
            else
               sAout[j] = String.valueOf(rng.nextGaussian()*stdDev[j] + mean[j]);
         }
         return sAout;
      }
   }

   public static int makeSubstData(JavaSparkContext sc, String inputFileName, String outputFileName)
   { 
   // Read from the input file an array of alfanumeric labels and numbers, compute the mean and variance of numbers and then
   // generate a new array of normally distributed random numbers
   // Compute the mean and variance (for verification) and write a .csv file with the generated data
   // Anonymize alfanumeric labels
   

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
      // System.out.println( inFileUq.take(5));
      
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
      
      JavaRDD<Vector> parsedInpV = inpFileTr.map(s -> s.split(",")).map(new ParseDouble()).map(dA -> Vectors.dense(dA));
      
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
      
      for (j = 0; j < numCols; j++)
      {
         mean[j] = summary.mean().apply(j);
         variance[j] = summary.variance().apply(j);
         stdDev[j] = Math.sqrt(variance[j]);         
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
      
      rng = new Random();
      
      JavaRDD<String[]> outData = inpFileTr.map(s -> s.split(",")).map(new Anon());
      
      System.out.println("outData:");
      for (String[] sA: outData.take(5))
      {
         for (String s: sA)
            System.out.print( s + "\t" );
         System.out.println();
      }
      
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

   } // makeSubstData()

   
   public static void main(String[] args)
   {
   
         SparkSession spark = SparkSession.builder().appName("Anonymizer").getOrCreate();

         JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
         
         makeSubstData(sc, "/home/bsrsharma/work/java/arran.csv", "/home/bsrsharma/work/java/subst.csv");

         System.out.println("Anonymizer finally ended");

         spark.stop();
         
   }  // main
}  // Anonymizer