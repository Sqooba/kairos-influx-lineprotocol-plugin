package org.kairosdb.plugin.influx;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import org.kairosdb.core.DataPoint;
import org.kairosdb.util.Tags;

public class InfluxMetric {
    private final String name;
    private final ImmutableSortedMap.Builder<String, String> tags;
    private final ImmutableMap.Builder<String, DataPoint> dataPoints;

    public InfluxMetric(String name) {
        this.name = name;
        tags = Tags.create();
        dataPoints = ImmutableMap.builder();
    }

    public void addTag(String tag, String value) {
        tags.put(tag, value);
    }

    public void addDataPoint(String field, DataPoint dp) {
        dataPoints.put(field, dp);
    }

    public String getName() {
        return name;
    }

    public ImmutableSortedMap<String, String> getTags() {
        return tags.build();
    }

    public ImmutableMap<String, DataPoint> getDataPoints() {
        return dataPoints.build();
    }
}
