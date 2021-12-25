package ru.bmstu.hadoop.lab2;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, CompositeKeyComparable, Text> {
    static final int DELAY_INDEX = 17;
    static final int AIRPORT_INDEX = 14;
    static final String COMMA_SPLITTER = ",";
    static final String STRIP_CHARS = "\"";
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
    }

    private String[] split(line String) {

    }
}
