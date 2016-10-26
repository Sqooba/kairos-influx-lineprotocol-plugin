package org.kairosdb.plugin.influx.udp;

import org.junit.Assert;
import org.junit.Test;
import org.kairosdb.core.datapoints.StringDataPoint;
import org.kairosdb.plugin.influx.InfluxLineProtocolParser;
import org.kairosdb.plugin.influx.InfluxMetric;

/**
 * Created by bperroud on 13/08/16.
 */
public class InfluxLineProtocolParserTest {

    @Test
    public void parse() throws Exception {

        InfluxMetric influxMetric = InfluxLineProtocolParser.parse("cpu\\ 1,host\\,1=test\\ 1,foo=bar,buggy1 1min=0.01,5min=0.05,15min=0.15,string=\"Test \\\" complex string = , yahoo!!\",buggy2 1471122447000000000");

        Assert.assertEquals("cpu 1", influxMetric.getName());

        Assert.assertEquals(2, influxMetric.getTags().size());
        Assert.assertTrue(influxMetric.getTags().containsKey("host,1"));
        Assert.assertEquals("test 1", influxMetric.getTags().get("host,1"));
        Assert.assertTrue(influxMetric.getTags().containsKey("foo"));
        Assert.assertEquals("bar", influxMetric.getTags().get("foo"));

        Assert.assertEquals(4, influxMetric.getDataPoints().size());
        Assert.assertTrue(influxMetric.getDataPoints().containsKey("1min"));
        Assert.assertEquals(0.01, influxMetric.getDataPoints().get("1min").getDoubleValue(), 0.000001);
        Assert.assertEquals(1471122447000L, influxMetric.getDataPoints().get("1min").getTimestamp());
        Assert.assertTrue(influxMetric.getDataPoints().containsKey("5min"));
        Assert.assertEquals(0.05, influxMetric.getDataPoints().get("5min").getDoubleValue(), 0.000001);
        Assert.assertTrue(influxMetric.getDataPoints().containsKey("15min"));
        Assert.assertEquals(0.15, influxMetric.getDataPoints().get("15min").getDoubleValue(), 0.000001);
        Assert.assertTrue(influxMetric.getDataPoints().containsKey("string"));
        Assert.assertEquals("Test \" complex string = , yahoo!!", ((StringDataPoint)influxMetric.getDataPoints().get("string")).getValue());

//        Ensure buggy lines don't crash the parsing. We're pretty permissive and tends to discard bad data
//        instead of failing.
        influxMetric = InfluxLineProtocolParser.parse("cpu");
        influxMetric = InfluxLineProtocolParser.parse("cpu,");
        influxMetric = InfluxLineProtocolParser.parse("cpu ");
        influxMetric = InfluxLineProtocolParser.parse("cpu, 5min=0.01,string=\"value with spaces and = and , and everything\"");
        influxMetric = InfluxLineProtocolParser.parse("cpu,host=test 5min=0.01 1471122447000000000");

    }

    @Test
    public void testBuggyLine() {

        double d = Double.parseDouble("1380549794928");
        String line = "kafka.server.BrokerTopicMetrics.BytesInPerSec,host=pmidevkafka4." +
                "pmi.com,topic=_schemas 15MinuteRate=0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
                "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000019443964741622819,1MinuteR" +
                "ate=0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
                "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002964393875,5MinuteRate=0.000000000000" +
                "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000" +
                "0000000000000000000000000000000000000000000000000000000000000000000000kafka.log.Log.LogEndOffset,host=pmidevkafka2.node.pmidev.ocean,partition=39,topic=__consumer_offsets value=1" +
                "5746i 1476348394796000000";

        InfluxMetric influxMetric = InfluxLineProtocolParser.parse(line);

    }

    @Test
    public void splitOnFirstEqualChar() {
        String[] res;

        res = InfluxLineProtocolParser.splitOnFirstEqualChar("key=value");
        Assert.assertEquals(2, res.length);
        Assert.assertEquals("key", res[0]);
        Assert.assertEquals("value", res[1]);

        res = InfluxLineProtocolParser.splitOnFirstEqualChar("key");
        Assert.assertEquals(0, res.length);

        res = InfluxLineProtocolParser.splitOnFirstEqualChar("key=");
        Assert.assertEquals(2, res.length);
        Assert.assertEquals("key", res[0]);
        Assert.assertEquals("", res[1]);

        res = InfluxLineProtocolParser.splitOnFirstEqualChar("key=v1=v2");
        Assert.assertEquals(2, res.length);
        Assert.assertEquals("key", res[0]);
        Assert.assertEquals("v1=v2", res[1]);
    }

    @Test
    public void readString() {
        InfluxLineProtocolParser.StringAndNewOffset stringAndNewOffset = InfluxLineProtocolParser.readString("complex\\ line\\ with\\ \"quotes \\\" and unescaped , inside\" test1", 0);
        Assert.assertEquals("complex line with \"quotes \" and unescaped , inside\"", stringAndNewOffset.getString());
    }

    @Test
    public void parseIntegers() throws Exception {

        InfluxMetric influxMetric = InfluxLineProtocolParser.parse("cpu1 1min=0i,5min=12312323i");

        Assert.assertEquals("cpu1", influxMetric.getName());

        Assert.assertEquals(0, influxMetric.getTags().size());

        Assert.assertEquals(2, influxMetric.getDataPoints().size());
        Assert.assertTrue(influxMetric.getDataPoints().containsKey("1min"));
        Assert.assertEquals(0L, influxMetric.getDataPoints().get("1min").getLongValue());
        Assert.assertTrue(influxMetric.getDataPoints().containsKey("5min"));
        Assert.assertEquals(12312323L, influxMetric.getDataPoints().get("5min").getLongValue());

    }
}