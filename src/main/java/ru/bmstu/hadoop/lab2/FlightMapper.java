package ru.bmstu.hadoop.lab2;

import com.sun.org.apache.xpath.internal.operations.String;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

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

        String records[] = split(line.toString());
        if (records.length != 2) {
            return;
        }
        for (int i = 0; i < records.length; i++) {
            records[i] = StringUtils.strip(records[i], STRIP_CHARS);
        }
    }

    static private String[] split(String line) {
        ArrayList<String> records = new ArrayList<>();
        int lineLen = line.length()
        StringBuilder currentStr = new StringBuilder();
        for (int i = 0; i < lineLen; i++) {
            char c = line.charAt(i);
            switch (c) {
                case '"':
                    break;
                case ',':

                    currentStr.append(c);
            }
        }
    }
}
