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
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Long, Double>> flightsData = flightsCSV
                .mapToPair(s -> {
                    String[] parameters = ParseUtils.ParseFlightsLogLine(s);
                    String originalAirportID = parameters[ParseUtils.FLIGHTS_ORIGIN_AIRPORT_ID_PARAM_NUMBER];
                    String destinationAirportID = parameters[ParseUtils.FLIGHTS_DEST_AIRPORT_ID_PARAM_NUMBER];
                    String delayString = parameters[ParseUtils.FLIGHTS_DELAY_PARAM_NUMBER];

                    Boolean isLate = delayString.isEmpty();
                    Long delay = (isLate) ? 0 : Long.parseLong(delayString);
                    return new Tuple2<>(new Tuple2<>(Integer.parseInt(originalAirportID),
                            Integer.parseInt(destinationAirportID)),
                            new Tuple3<>(delay, (isLate || delay != 0) ? 1 : 0, 1));
                }).reduceByKey((a, b) -> new Tuple3<>(Math.max(a._1(), b._1()),
                        a._2() + b._2(),
                        a._3() + b._3()))
                .mapToPair(s -> new Tuple2<>(s._1(),
                        new Tuple2<>(s._2()._1(), new Double((double)s._2()._2() / s._2()._3() * 100))));

        JavaRDD<String> airportsCSV = sc.textFile("L_AIRPORT_ID.csv");
        JavaPairRDD<Integer, String> airportsData = airportsCSV.mapToPair(s -> {
            String[] parameters = ParseUtils.ParseAirportsListLine(s);
            return new Tuple2<>(Integer.parseInt(parameters[ParseUtils.AIRPORTS_AIRPORT_ID_PARAM_NUMBER]),
                    parameters[ParseUtils.AIRPORTS_AIRPORT_DESCRIPTION_PARAM_NUMBER]);
        });
        Map<Integer, String> stringAirportDataMap = airportsData.collectAsMap();
        final Broadcast<Map<Integer, String>> airportsBroadcasted = sc.broadcast(stringAirportDataMap);

        JavaPairRDD<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>, Tuple2<Long, Double>> result =
                flightsData.mapToPair(s -> new Tuple2<>(new Tuple2<>(
                        new Tuple2<>(s._1()._1(), airportsBroadcasted.value().get(s._1()._1())),
                        new Tuple2<>(s._1()._2(), airportsBroadcasted.value().get(s._1()._2()))),
                        s._2()));

        result.saveAsTextFile();
    }

//    private static Tuple2<Tuple2<Integer, Integer>, Long> GetNewFlightKeyValuePair (String line) {
//        String[] parameters = ParseUtils.ParseFlightsLogLine(line);
//        String originalAirportID = parameters[ParseUtils.FLIGHTS_LOG_ORIGIN_AIRPORT_ID_PARAM_NUMBER];
//        String destinationAirportID = parameters[ParseUtils.FLIGHTS_LOG_DEST_AIRPORT_ID_PARAM_NUMBER];
//        String delayString = parameters[ParseUtils.FLIGHTS_LOG_DELAY_PARAM_NUMBER];
//        Long delay = (delayString.isEmpty()) ? 0 : Long.parseLong(delayString);
//        return new Tuple2<>(new Tuple2<>(Integer.parseInt(originalAirportID), Integer.parseInt(destinationAirportID)), delay);
//    }
//
//    private static Tuple2<Integer, String> GetNewAirportsKeyValuePair (String line) {
//        String[] parameters = ParseUtils.ParseAirportsListLine(line);
//        return new Tuple2<>(Integer.parseInt(parameters[ParseUtils.AIRPORTS_LIST_AIRPORT_ID_PARAM_NUMBER]),
//                parameters[ParseUtils.AIRPORTS_LIST_AIRPORT_DESCRIPTION_PARAM_NUMBER]);
//    }
}
