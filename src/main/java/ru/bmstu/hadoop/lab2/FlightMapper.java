package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class FlightMapper extends Mapper<LongWritable, Text, CompositeKeyComparable, Text> {
    static final int DELAY_INDEX = 17;
    static final int AIRPORT_INDEX = 14;
    @Override
    protected void map(LongWritable k, Text line, Context ctx) throws
            IOException, InterruptedException {
        if (k.get() == 0) {
            return;
        }

        String records[] = split(line.toString());
        String delay = records[DELAY_INDEX];
        int airportId;

        try {
            if (delay.isEmpty() || Integer.parseInt(delay) <= 0) {
                return;
            }
            airportId = Integer.parseInt(records[AIRPORT_INDEX]);
        } catch (NumberFormatException e) {
            return;
        }

        CompositeKeyComparable key = new CompositeKeyComparable(airportId, CompositeKeyComparable.FLIGHT_KEY);
        ctx.write(key, new Text(delay));
    }

    static private String[] split(String line) {
        ArrayList<String> records = new ArrayList<>();
        int lineLen = line.length();
        StringBuilder currentStr = new StringBuilder();
        for (int i = 0; i < lineLen; i++) {
            char c = line.charAt(i);
            switch (c) {
                case '"':
                    break;
                case ',':
                    records.add(currentStr.toString());
                    currentStr = new StringBuilder();
                    break;
                default:
                    currentStr.append(c);
            }
        }
    }
}
