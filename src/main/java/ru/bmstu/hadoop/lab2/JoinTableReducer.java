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
        double min = Integer.MAX_VALUE, sum = 0, max = 0;
        int n = 0;
        int i = 0;
        while (iter.hasNext()) {
            double delay = 0;
            try {
                delay = Double.parseDouble(iter.next().toString());
            } catch (NumberFormatException e) {
                i = n;
            }
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
            Iterator<Text> it = values.iterator();
            it.next();
            String v = "";
            if (it.hasNext()) {
                v = it.next().toString();
            }

            ctx.write(airportName, new Text(value + " i = " + i + " " + v));
        } else {
            ctx.write(airportName, new Text(key.toString()));
        }
    }
}
