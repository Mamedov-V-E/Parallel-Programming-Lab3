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
        JavaPairRDD<Tuple2<String, String>, Long> flightsData = flights
                .mapToPair(SparkApp::GetNewFlightKeyValuePair);

        stringAirportDataMap = 
        final Broadcast<Map<String, AirportData>> airportsBroadcasted = sc.broadcast(stringAirportDataMap);
    }

    private static Tuple2<Tuple2<String, String>, Long> GetNewFlightKeyValuePair(String line) {
        String[] parameters = ParseUtils.ParseFlightsLogLine(line);
        String originalAirportID = parameters[ParseUtils.FLIGHTS_LOG_ORIGIN_AIRPORT_ID_PARAM_NUMBER];
        String destinationAirportID = parameters[ParseUtils.FLIGHTS_LOG_DEST_AIRPORT_ID_PARAM_NUMBER];
        String delayString = parameters[ParseUtils.FLIGHTS_LOG_DELAY_PARAM_NUMBER];
        Long delay = (delayString.isEmpty()) ? 0 : Long.parseLong(delayString);
        return new Tuple2<>(new Tuple2<>(originalAirportID, destinationAirportID), delay);
    }
}
