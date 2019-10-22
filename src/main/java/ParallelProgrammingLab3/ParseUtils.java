package ParallelProgrammingLab3;

class ParseUtils {
    static final Integer FLIGHTS_LOG_ORIG_AIRPORT_ID_PARAM_NUMBER = 14;
    static final Integer FLIGHTS_LOG_DELAY_PARAM_NUMBER = 17;
    static final Integer AIRPORTS_LIST_AIRPORT_ID_PARAM_NUMBER = 0;
    static final Integer AIRPORTS_LIST_AIRPORT_DESCRIPTION_PARAM_NUMBER = 1;
    private static final String FLIGHTS_LOG_DELIMITER = ",";
    private static final String AIRPORTS_LIST_DELIMITER = "\",\"";
    private static final String QUOTES_SYMBOL = "\"";
    static final Integer AIRPORTS_LIST_HEADER_LINE_NUMBER = 0;
    static final Integer FLIGHTS_LOG_HEADER_LINE_NUMBER = 0;
    static final Byte AIRPORTS_LIST_CODE = 0;
    static final Byte FLIGHTS_LOG_CODE = 1;

    static String[] ParseFlightsLogLine(String line) {
        return ParseCSVLine(line, FLIGHTS_LOG_DELIMITER);
//        String[] parameters = ParseCSVLine(line, FLIGHTS_LOG_DELIMITER);
//        return new String[] { parameters[FLIGHTS_LOG_AIRPORT_ID_PARAM_NUMBER], parameters[FLIGHTS_LOG_DELAY_PARAM_NUMBER]};
    }

    static String[] ParseAirportsListLine(String line) {
        return ParseCSVLine(line, AIRPORTS_LIST_DELIMITER);
    }

    private static String[] ParseCSVLine(String line, String delimiter) {
        String[] parameters = line.split(delimiter);
        for (int i = 0; i < parameters.length; ++i) {
            parameters[i] = parameters[i].replaceAll(QUOTES_SYMBOL, "");
        }
        return parameters;
    }
}
