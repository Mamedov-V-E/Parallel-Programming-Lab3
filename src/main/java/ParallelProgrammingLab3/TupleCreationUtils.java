package ParallelProgrammingLab3;

import scala.Tuple2;
import scala.Tuple3;

public class TupleCreationUtils {
    public static Tuple2<Tuple2<Integer, Integer>, Tuple3<Double, Integer, Integer>>
    CreateFlightsPair(String line) {
        String[] parameters = ParseUtils.ParseFlightsLogLine(line);
        String originalAirportID = parameters[ParseUtils.FLIGHTS_ORIGIN_AIRPORT_ID_PARAM_NUMBER];
        String destinationAirportID = parameters[ParseUtils.FLIGHTS_DEST_AIRPORT_ID_PARAM_NUMBER];
        String delayString = parameters[ParseUtils.FLIGHTS_DELAY_PARAM_NUMBER];

        boolean isLate = delayString.isEmpty();
        Double delay = (isLate) ? 0 : Double.parseDouble(delayString);
        return new Tuple2<>(new Tuple2<>(Integer.parseInt(originalAirportID),
                Integer.parseInt(destinationAirportID)),
                new Tuple3<>(delay, (isLate || (delay > 0)) ? 1 : 0, 1));
    }

    public static Tuple2<Integer, String> CreateAirportsPair(String line) {
        String[] parameters = ParseUtils.ParseAirportsListLine(line);
        return new Tuple2<>(Integer.parseInt(parameters[ParseUtils.AIRPORTS_AIRPORT_ID_PARAM_NUMBER]),
                parameters[ParseUtils.AIRPORTS_AIRPORT_DESCRIPTION_PARAM_NUMBER]);
    }
}