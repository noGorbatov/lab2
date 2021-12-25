package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class AirportMapper extends Mapper<LongWritable, Text, CompositeKeyComparable, Text> {
    public final String STRIP_CHARS = "\"";
    public final String COMMA_SPLITTER = ",";
    public final int RECORD_NUMBER = 2;
    public final int KEY = 0;
    public final int VALUE = 1;
    @Override
    protected void map(LongWritable k, Text line, Context ctx) throws
            IOException, InterruptedException {
        if (k.get() == 0) {
            return;
        }

        String records[] = StringUtils.split(line.toString(), COMMA_SPLITTER, RECORD_NUMBER);
        if (records.length != 2) {
            return;
        }
        for (int i = 0; i < records.length; i++) {
            records[i] = StringUtils.strip(records[i], STRIP_CHARS);
        }

        int airportId;
        try {
            airportId = Integer.parseInt(records[KEY]);
        }
        catch (NumberFormatException e) {
            return;
        }

        CompositeKeyComparable key = new CompositeKeyComparable(CompositeKeyComparable.AIRPORT_KEY, airportId);
        ctx.write(key, new Text(records[VALUE]));
    }
}
