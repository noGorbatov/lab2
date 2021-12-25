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
        Text airportName = iter.next();
        int min = Integer.MAX_VALUE, sum = 0, max = 0, n = 0;
        while (iter.hasNext()) {
            int delay = Integer.parseInt(iter.next().toString());
            if (delay < min) {
                min = delay;
            }
            if (delay > max) {
                max = delay;
            }
            sum += delay;
            n++;
        }

        if (n != 0) {
            String value = "min = " + min + ", max = " + max + ", average = " + sum/n;
            ctx.write(airportName, new Text(value));
        }
    }
}
