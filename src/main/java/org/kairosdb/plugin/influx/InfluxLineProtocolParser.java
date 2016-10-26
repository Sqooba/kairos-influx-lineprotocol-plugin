package org.kairosdb.plugin.influx;

import org.apache.commons.lang3.StringUtils;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.StringDataPoint;
import org.kairosdb.plugin.influx.InfluxMetric;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class InfluxLineProtocolParser {

    private static final char COMMA = ',';
    private static final char ESCAPE = '\\';
    private static final char SPACE = ' ';
    private static final char EQUAL = '=';
    private static final char DOUBLE_QUOTE = '"';
    private static final char I = 'i';

    public static InfluxMetric parse(String line) {

//        https://docs.influxdata.com/influxdb/v0.13/write_protocols/line/
        // measurement [fields[ timestamp]]
        // with measurement name[,tag=value]*
        // with name having spaces and comma escaped

        long executionTimestampNS = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());

        int offset = 0;
        StringAndNewOffset tmp;

        tmp = readString(line, offset);

        if (StringUtils.isEmpty(tmp.string)) {
            return null;
        }

        String name = tmp.string;
        offset = tmp.newOffset;

        final InfluxMetric influxMetric = new InfluxMetric(name);

//        Parse the tags
        while (offset < line.length() && line.charAt(offset) == COMMA) {
            tmp = readString(line, offset + 1);
            String[] tagPart = splitOnFirstEqualChar(tmp.string);
            if (tagPart.length == 2) {
                influxMetric.addTag(tagPart[0], tagPart[1]);
            }
            offset = tmp.newOffset;
        }


//        Parse the fields
//        Since the timestamp is at the end of the line and a DataPoint in KairosDB need the timestamp,
//        I have to defer the datapoint (field) creation
        Map<String, String> datapoints = new HashMap<String, String>();

        boolean first = true;

        while (offset < line.length() && (first || line.charAt(offset) == COMMA)) {
            first = false;
            tmp = readString(line, offset + 1);
            String[] fieldPart = splitOnFirstEqualChar(tmp.string);
            if (fieldPart.length == 2) {
//                    Parse the value: starting with " is a string, ending with i is an integer, t/true f/false case insensitive is a boolean
                String key = fieldPart[0];
                String value = fieldPart[1];

                if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value)) {
                    continue;
                }
                datapoints.put(key, value);
            }
            offset = tmp.newOffset;
        }


//        read timestamp
        long timestampNS = executionTimestampNS;
        if (offset < line.length()) {
            tmp = readString(line, offset + 1);
            try {
                timestampNS = TimeUnit.NANOSECONDS.toMillis(Long.parseLong(tmp.string));
            } catch (NumberFormatException e) {
//                TODO: Handle this error.
            }
        }

//        Convert fields to datapoint. Almost there!
        for (Map.Entry<String, String> datapoint : datapoints.entrySet()) {

            String field = datapoint.getKey();
            String value = datapoint.getValue();

            DataPoint dp = null;
            if (value.charAt(0) == DOUBLE_QUOTE) {
                dp = new StringDataPoint(timestampNS, value.substring(1, value.length() - 1));
            } else if (value.charAt(value.length() - 1) == I) {
                dp = new LongDataPoint(timestampNS, Long.parseLong(value.substring(0, value.length() - 1)));
            } else if (isTrueFalse(value.charAt(0))) {
                // TODO: what to do with that?
            } else {
                dp = new DoubleDataPoint(timestampNS, Double.parseDouble(value));
            }

            if (dp != null) {
                influxMetric.addDataPoint(field, dp);
            }

        }
        return influxMetric;
    }

    public static StringAndNewOffset readString(String line, int offset) {

        StringBuilder sb = new StringBuilder();
        boolean escaped = false;
        boolean quoted = false;
        char currentChar;

        while (offset < line.length()) {

            currentChar = line.charAt(offset);

            if ((currentChar == COMMA || currentChar == SPACE) && !escaped && !quoted) {
                break;
            }
            if (currentChar == ESCAPE && !escaped) {
                escaped = true;
            } else {
                if (currentChar == DOUBLE_QUOTE && !escaped) {
                    quoted = !quoted;
                }
                escaped = false;
                sb.append(currentChar);
            }
            offset++;
        }

        if (quoted) {
            throw new IllegalStateException("Unclosed quote");
        }
        return new StringAndNewOffset(sb.toString(), offset);
    }

    public static class StringAndNewOffset {
        private final String string;
        private final int newOffset;

        public StringAndNewOffset(String string, int newOffset) {
            this.string = string;
            this.newOffset = newOffset;
        }

        public String getString() {
            return string;
        }
    }

    private static boolean isTrueFalse(char value) {
        return value == 't' ||
                value == 'T' ||
                value == 'f' ||
                value == 'F';
    }

    public static String[] splitOnFirstEqualChar(String kevValue) {

        int pos = kevValue.indexOf(EQUAL);
        if (pos > 0) {
//                    Parse the value: starting with " is a string, ending with i is an integer, t/true f/false case insensitive is a boolean
            String key = kevValue.substring(0, pos);
            String value = pos < kevValue.length() - 1 ? kevValue.substring(pos + 1, kevValue.length()) : "";

            return new String[]{key, value};
        }

        return new String[0];
    }
}
