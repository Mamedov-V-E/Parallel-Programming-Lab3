package ParallelProgrammingLab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Map;

public class SparkApp {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> flightsCSV = sc.textFile("664600583_T_ONTIME_sample.csv");
        String flightsHeader = flightsCSV.first();
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> flightsData = flightsCSV
                .filter(s -> !s.equals(flightsHeader))
                .mapToPair(PairCreationUtils::CreateFlightsPair)
                .reduceByKey((a, b) -> new Tuple3<>(Math.max(a._1(), b._1()),
                        a._2() + b._2(),
                        a._3() + b._3()))
                .mapToPair(s -> new Tuple2<>(s._1(),
                        new Tuple2<>(s._2()._1(), new Double((double)s._2()._2() / s._2()._3() * 100))));

        JavaRDD<String> airportsCSV = sc.textFile("L_AIRPORT_ID.csv");
        String airportsHeader = airportsCSV.first();
        JavaPairRDD<Integer, String> airportsData = airportsCSV.filter(s -> !s.equals(airportsHeader))
                .mapToPair(PairCreationUtils::CreateAirportsPair);
        Map<Integer, String> stringAirportDataMap = airportsData.collectAsMap();
        final Broadcast<Map<Integer, String>> airportsBroadcasted = sc.broadcast(stringAirportDataMap);

        JavaPairRDD<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>, Tuple2<Double, Double>> result =
                flightsData.mapToPair(s -> new Tuple2<>(new Tuple2<>(
                        new Tuple2<>(s._1()._1(), airportsBroadcasted.value().get(s._1()._1())),
                        new Tuple2<>(s._1()._2(), airportsBroadcasted.value().get(s._1()._2()))),
                        s._2()));

        result.saveAsTextFile("result");
    }
}
