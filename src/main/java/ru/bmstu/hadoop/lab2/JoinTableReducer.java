package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


public class JoinTableReducer extends Reducer<CompositeKeyComparable, Text, Text, Text> {
    @Override
    protected void reduce(CompositeKeyComparable key, Iterable<Text> values, Context ctx) throws
            IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        String airportName = iter.next().toString();
        int min = Integer.MAX_VALUE, sum = 0, max = 0, n = 0;
        while (iter.hasNext()) {
            int delay = Integer.parseInt(iter.next().toString());
            if (delay < min) {
                min = delay;
            }
            if (delay > max) {
                max = delay;
            }
            sum += 
        }

        if (n != 0) {

        }
    }
}
