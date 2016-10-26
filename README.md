KairosDB Plugin to support Influx line protocol
=============

This project aims at enhancing KairosDB to support [influx line protocol](https://docs.influxdata.com/influxdb/v0.13/write_protocols/line/)
"natively", i.e. using [telegraf](https://influxdata.com/time-series-platform/telegraf/) directly
to send data point to KairosDB.

# Installation

## Build
This is a maven project, so just build it

```
mvn clean install
```

## Distribute

The resulting jar, namely `target/kairos-influx-lineprotocol-1.0.0-SNAPSHOT.jar`, has to be copied
into the `lib` folder of your KairosDB installation, for instance `/opt/kairosdb/lib`

## Configure

Copy `src/main/resources/kairos-influx.properties` into the `conf` fodler of your KairosDB installation,
for instance `/opt/kairosdb/conf` and update it accordingly to your need.

## Restart KairosDB

You're all set, you can restart your KairosDB instance and try it!

# Testing

```
echo -n "test,host=localhost,foo=bar value1=1i,value2=2i" | nc -4u localhost 8089
```

Use KairosDB to ensure you can see this data!