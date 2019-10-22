package ParallelProgrammingLab3;

class ParseUtils {
    static final Integer FLIGHTS_ORIGIN_AIRPORT_ID_PARAM_NUMBER = 11;
    static final Integer FLIGHTS_DEST_AIRPORT_ID_PARAM_NUMBER = 14;
    static final Integer FLIGHTS_DELAY_PARAM_NUMBER = 17;
    static final Integer AIRPORTS_AIRPORT_ID_PARAM_NUMBER = 0;
    static final Integer AIRPORTS_AIRPORT_DESCRIPTION_PARAM_NUMBER = 1;
    private static final String FLIGHTS_DELIMITER = ",";
    private static final String AIRPORTS_DELIMITER = "\",\"";
    private static final String QUOTES_SYMBOL = "\"";
    static final Integer AIRPORTS_HEADER_LINE_NUMBER = 0;
    static final Integer FLIGHTS_HEADER_LINE_NUMBER = 0;
    static final Byte AIRPORTS_CODE = 0;
    static final Byte FLIGHTS_CODE = 1;

    static String[] ParseFlightsLogLine(String line) {
        return ParseCSVLine(line, FLIGHTS_DELIMITER);
//        String[] parameters = ParseCSVLine(line, FLIGHTS_LOG_DELIMITER);
//        return new String[] { parameters[FLIGHTS_LOG_AIRPORT_ID_PARAM_NUMBER], parameters[FLIGHTS_LOG_DELAY_PARAM_NUMBER]};
    }

    static String[] ParseAirportsListLine(String line) {
        return ParseCSVLine(line, AIRPORTS_DELIMITER);
    }

    private static String[] ParseCSVLine(String line, String delimiter) {
        String[] parameters = line.split(delimiter);
        for (int i = 0; i < parameters.length; ++i) {
            parameters[i] = parameters[i].replaceAll(QUOTES_SYMBOL, "");
        }
        return parameters;
    }
}
