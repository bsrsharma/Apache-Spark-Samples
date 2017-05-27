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
   public static String unquoteRemoveEmbeddedCommas (String s)
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
   }  // unquoteremoveEmbeddedCommas
  

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
   
   static double mean[], stdDev[];
   static String typeStrings[];
   static Random rng;
   
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
      
      JavaRDD<Vector> parsedInpV = inpFile.map(s -> s.split(",")).map(new ParseDouble()).map(dA -> Vectors.dense(dA));
      
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
      
      //double mean[] = new double[numCols];
      mean = new double[numCols];
      double variance[] = new double[numCols];
      //double stdDev[] = new double[numCols];
      stdDev = new double[numCols];
      
      for (j = 0; j < numCols; j++)
      {
         mean[j] = summary.mean().apply(j);
         variance[j] = summary.variance().apply(j);
         stdDev[j] = Math.sqrt(variance[j]);         
      }
      
      //String typeStrings[] = new String[numCols];
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
      
      //Random rng = new Random();
      rng = new Random();
      
      JavaRDD<String[]> outData = inpFile.map(s -> s.split(",")).map(new Anon());
      
      System.out.println("outData:");
      for (String[] sA: outData.take(5))
      {
         for (String s: sA)
            System.out.print( s + "\t" );
         System.out.println();
      }       
      
/*******************************************************************************      
      // double arrn[][] = new double[numRows][numCols];
      List<String> AlistOfStr= new ArrayList<String>();
      
      AlistOfStr = inpFile.collect();
      System.out.println(AlistOfStr.get(0) + "....." + AlistOfStr.get(numRows-1));
      
      //System.out.println("DenseMatrix randn( " + numRows + " , " + numCols + " )");

      //double values[] = new double[numRows*numCols];
      
      //DenseMatrix origData = new DenseMatrix(numRows, numCols, values);
      //origData = origData.randn(numRows, numCols, rng);

      System.out.println("print out a few rows generated....");

      String[] aOfStr  = new String[numCols];
      
      for (i = 0; i < Math.min(4, numRows); i++)
      {
         aOfStr = AlistOfStr.get(i).split(",");
         for (j = 0; j < numCols; j++)
         {
            if (typeStrings[j] == "String")
               System.out.print(makeAnonStr(aOfStr[j]) + "\t");
            else
               System.out.print(rng.nextGaussian() + "\t");
         }
         System.out.println();
      }
	  
      System.out.println("using means and variances of:");

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

      //shiftAndScaleMat(numRows, numCols, mean, stdDev, origData, arrn);

      System.out.println("print out a few rows of transformed data" );

      for (i = 0; i < Math.min(4, numRows); i++)
      {
         aOfStr = AlistOfStr.get(i).split(",");      
         for (j = 0; j < numCols; j++)
         {
            if (typeStrings[j] == "String")
               System.out.print(makeAnonStr(aOfStr[j]) + "\t");
            else         
               System.out.print((rng.nextGaussian()*stdDev[j] + mean[j]) + "\t");
         }
         System.out.println();
      }

      try
      {
         PrintWriter fileHandle = new PrintWriter(outputFileName);

         for (i = 0; i < numRows; i++)
         {
            aOfStr = AlistOfStr.get(i).split(",");      
            if (numCols > 1)
            {
               for (j = 0; j < numCols - 1; j++)
               {
                  if (typeStrings[j] == "String")
                     fileHandle.print(makeAnonStr(aOfStr[j]));
                  else                           
                     fileHandle.print(String.valueOf(rng.nextGaussian()*stdDev[j] + mean[j]));
                  fileHandle.print(",");
               }
            }
            if (typeStrings[j] == "String")
               fileHandle.println(makeAnonStr(aOfStr[numCols - 1]));
            else                           
               fileHandle.println(String.valueOf(rng.nextGaussian()*stdDev[numCols - 1] + mean[numCols - 1]));            
         }
         fileHandle.close();
      }
*******************************************************************************/

      try
      {
         PrintWriter fileHandle = new PrintWriter(outputFileName);
         
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
               else
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