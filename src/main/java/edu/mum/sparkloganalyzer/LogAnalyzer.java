package edu.mum.sparkloganalyzer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

/**
 * 
 * @author najus
 *
 */
public class LogAnalyzer {
	private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

	private static class ValueComparator<K, V> implements Comparator<Tuple2<K, V>>, Serializable {
		
		private static final long serialVersionUID = 5143521232671751388L;
		
		private Comparator<V> comparator;

		public ValueComparator(Comparator<V> comparator) {
			this.comparator = comparator;
		}

		@Override
		public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
			return comparator.compare(o1._2(), o2._2());
		}
	}

	public static void main(String[] args) throws IOException {

		// Create a Spark Context.
		SparkConf conf = new SparkConf().setAppName("Log Analyzer").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the text file into Spark.
		if (args.length == 0) {
			System.out.println("Must specify an access logs file.");
			System.exit(-1);
		}
		String logFile = args[0];
		String outputFile = args[1];
		File output = new File(outputFile);
		if (!output.exists()) {
			output.createNewFile();
		}

		FileWriter fw = new FileWriter(output.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		JavaRDD<String> logLines = sc.textFile(logFile);

		// Convert the text log lines to ApacheAccessLog objects and cache them
		// since multiple transformations and actions will be called on that
		// data.
		JavaRDD<ApacheAccessLog> accessLogs = logLines.map(ApacheAccessLog::parseFromLogLine).cache();

		// Calculate statistics based on the content size.
		// Note how the contentSizes are cached as well since multiple actions
		// are called on that RDD.
		JavaRDD<Long> contentSizes = accessLogs.map(ApacheAccessLog::getContentSize).cache();
		System.out.println(String.format("Content Size Avg: %s, Min: %s, Max: %s",
				contentSizes.reduce(SUM_REDUCER) / contentSizes.count(), contentSizes.min(Comparator.naturalOrder()),
				contentSizes.max(Comparator.naturalOrder())));
		bw.write(String.format("Content Size Avg: %s, Min: %s, Max: %s",
				contentSizes.reduce(SUM_REDUCER) / contentSizes.count(), contentSizes.min(Comparator.naturalOrder()),
				contentSizes.max(Comparator.naturalOrder())));

		// Compute Response Code to Count.
		List<Tuple2<Integer, Long>> responseCodeToCount = accessLogs
				.mapToPair(log -> new Tuple2<>(log.getResponseCode(), 1L)).reduceByKey(SUM_REDUCER).take(100);
		System.out.println(String.format("Response code counts: %s", responseCodeToCount));
		bw.write(String.format("\nResponse code counts: %s", responseCodeToCount));

		// Any IPAddress that has accessed the server more than 10 times.
		List<String> ipAddresses = accessLogs.mapToPair(log -> new Tuple2<>(log.getIpAddress(), 1L))
				.reduceByKey(SUM_REDUCER).filter(tuple -> tuple._2() > 10).map(Tuple2::_1).take(100);
		System.out.println(String.format("IPAddresses > 10 times: %s", ipAddresses));
		bw.write(String.format("\nIPAddresses > 10 times: %s", ipAddresses));

		// Top Endpoints.
		List<Tuple2<String, Long>> topEndpoints = accessLogs.mapToPair(log -> new Tuple2<>(log.getEndpoint(), 1L))
				.reduceByKey(SUM_REDUCER).top(10, new ValueComparator<>(Comparator.<Long> naturalOrder()));
		System.out.println(String.format("Top Endpoints: %s", topEndpoints));
		bw.write(String.format("\nTop Endpoints: %s", topEndpoints));

		bw.close();
		sc.close();
		sc.stop();
	}
}
