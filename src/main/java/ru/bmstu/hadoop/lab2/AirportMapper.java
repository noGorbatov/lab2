package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, Text, CompositeKeyComparable, Text> {
    final String stripChars = "\"";
    final String commaSplitter = ",";
    final int RECORD_NUMBER = 2;
    @Override
    protected void map(LongWritable k, Text line, Context ctx) throws
            IOException, InterruptedException {
        if (k.get() == 0) {
            return;
        }

        String record = StringUtils.strip(line.toString(), stripChars);

        String records[] = StringUtils.split(record, commaSplitter, RECORD_NUMBER);

    }
}
