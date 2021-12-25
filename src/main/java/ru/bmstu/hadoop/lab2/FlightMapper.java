package ru.bmstu.hadoop.lab2;

import org.apache.commons.lang3.StringUtils;
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
        String
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
