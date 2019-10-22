package ParallelProgrammingLab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

public class SparkApp {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> flights = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaPairRDD<Tuple2<String, String>, Tuple2 <Long, Byte>> flightsData = flights
                .mapToPair(s -> new Tuple2<>(new Tuple2<>()));
    }

    private static Tuple2<Tuple2<String, String>, Tuple2<Long, Byte>> GetNewFlightKeyValuePair(String line) {
        String[] parameters = ParseUtils.ParseFlightsLogLine(line);
        String Original
        return new Tuple2<>(new Tuple2<>(parameters[10], parameters[13]), new Tuple2<>())
    }
}
