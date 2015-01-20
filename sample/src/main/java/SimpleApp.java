/* SimpleApp.java */
import java.util.Arrays;

import org.apache.spark.api.java.*;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class SimpleApp {
    static SparkConf conf;
    static JavaSparkContext sc;
    static JavaRDD<String> logData;
    static String logFile = "README.md";
    static JavaPairRDD<String, String> rdd;
    static int numberOfPartitions = 4;

    public static void main(String[] args) {
        conf = new SparkConf().setAppName("Lks21c Application");
        sc = new JavaSparkContext(conf);

        sorting();
    }

    private static void sorting() {
    	logData = sc.textFile("in", numberOfPartitions);
    	JavaPairRDD<Integer, Integer> scorePerMemberKey = logData.mapToPair(new PairFunction<String, Integer, Integer>() {
			public Tuple2<Integer, Integer> call(String t) throws Exception {
				String[] arr = t.trim().split(",");
				Integer score = Integer.valueOf(arr[1].trim());
				Integer memberKey = Integer.valueOf(arr[0].trim());
				return new Tuple2<Integer, Integer>(score, memberKey);
			}
		}).partitionBy(new HashPartitioner(numberOfPartitions)).persist(StorageLevel.MEMORY_ONLY());
    	System.out.println("scorePerMemberKey count = " + scorePerMemberKey.count());
    	System.out.println("scorePerMemberKey partition size = " + scorePerMemberKey.partitions().size());

    	JavaPairRDD<Integer, Integer> sortedScorePerMemberKey = scorePerMemberKey.sortByKey();
    	sortedScorePerMemberKey.saveAsTextFile("out");
	}

	private static void countLines() {
        logData = sc.textFile(logFile);
        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("a");
            }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("b");
            }
        }).count();

        System.out.println("lks Lines with a: " + numAs + ", lines with b: "
                + numBs);
    }

    private static void createRDD() {
        PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String x) {
                return new Tuple2(x.split(" ")[0], x);
            }
        };
        rdd = sc.textFile(logFile).mapToPair(keyData);
        System.out.println("Create RDD complete");
    }

    private static void longFilter() {
        createRDD();
        Function<Tuple2<String, String>, Boolean> longWordFilter = new Function<Tuple2<String, String>, Boolean>() {
            public Boolean call(Tuple2<String, String> input) {
                return (input._2().length() < 20);
            }
        };
        JavaPairRDD<String, String> result = rdd.filter(longWordFilter);
        System.out.println("longFilter complete");
    }

    private static void wordCount() {
        logData = sc.textFile(logFile);

        JavaRDD<String> words = logData.flatMap(new FlatMapFunction<String, String>() {
          public Iterable<String> call(String x) { return Arrays.asList(x.split(" ")); }
        });
        JavaPairRDD<String, Integer> result = words.mapToPair(
          new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String x) { return new Tuple2(x, 1); }
        }).reduceByKey(
          new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) { return a + b; }
        });

        result.saveAsTextFile("result");
    }
}
