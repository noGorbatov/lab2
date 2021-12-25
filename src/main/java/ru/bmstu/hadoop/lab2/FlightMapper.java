package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, CompositeKeyComparable, Text> {
    static final int DELAY_INDEX = 17;
    static final int AIRPORT_INDEX = 14;
    @Override
    protected void map(LongWritable k, Text line, Context ctx) throws
            IOException, InterruptedException {
        if (k.get() == 0) {
            return;
        }

        String[] records = line.toString().split(",");
        String delay = records[DELAY_INDEX];
        int airportId = Integer.parseInt(records[AIRPORT_INDEX]);
        if (delay.isEmpty() || Double.parseDouble(delay) <= 0.f) {
            return;
        }

        CompositeKeyComparable key = new CompositeKeyComparable(airportId, CompositeKeyComparable.FLIGHT_KEY);
        ctx.write(key, new Text(delay));
    }
}
