package edu.mum.sparkloganalyzer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * @author najus
 *
 */
public class LogAnalyzer {
	public static void main(String[] args) {
		
		// Create a Spark Context.
		SparkConf conf = new SparkConf().setAppName("Log Analyzer");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the text file into Spark.
		if (args.length == 0) {
			System.out.println("Must specify an access logs file.");
			System.exit(-1);
		}
		String logFile = args[0];
		JavaRDD<String> logLines = sc.textFile(logFile);

		// TODO: Insert code here for processing logs.

		sc.stop();
	}
}
