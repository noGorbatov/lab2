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
        double min = Integer.MAX_VALUE, sum = 0, max = 0;
        int n = 0;

        while (iter.hasNext()) {
            Text v = iter.next();
            double delay = Double.parseDouble(v.toString());
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
            String value = "\nmin = " + min +
                    "\nmax = " + max +
                    "\naverage = " + sum/n +
                    "\nnumber of delayed flights = " + n;
            ctx.write(new Text(airportName), new Text(value));
        }
    }
}
