package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;


public class JoinTableReducer extends Reducer<CompositeKeyComparable, Text, Text, Text> {
    @Override
    protected void reduce(CompositeKeyComparable key, Iterable<Text> values, Context ctx) throws
            IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        String airportName = iter.next().toString();
        double min = Integer.MAX_VALUE, sum = 0, max = 0;
        int n = 0;
        int i = 0;
        Text v = new Text();
        ArrayList<CompositeKeyComparable> keys = new ArrayList<>();
        keys.add(key);
        System.out.println("Airportname " + airportName);

        while (iter.hasNext()) {
            v = iter.next();
            double delay = Double.parseDouble(v.toString());
            System.out.println(Integer.toString(n) + " " + delay);
            if (delay < min) {
                min = delay;
            }
            if (delay > max) {
                max = delay;
            }
            sum += delay;
            n++;
            keys.add(key);
        }

        if (n != 0) {
            String value = "min = " + min + ", max = " + max + ", average = " + sum/n;
//            Iterator<Text> it = values.iterator();
//            it.next();
//            String v = "";
//            if (it.hasNext()) {
//                v = it.next().toString();
//            }

            ctx.write(new Text(airportName), new Text(value + " i = " + i + " n = " + n + " " + key.toString()));
//        } else {
//            boolean diff = false;
//            for (int j = 0; j < keys.size(); j++) {
//                for (int k = 0; k < keys.size(); k++) {
//                    if (j == k) {
//                        continue;
//                    }
//                    if (keys.get(j).getAirportId() != keys.get(k).getAirportId()) {
//                        diff = true;
//                    }
//                }
//            }
//            if (diff) {
//                ctx.write(airportName, new Text(key.toString() + " diff = true"));
//            } else {
//                ctx.write(airportName, new Text(key.toString() + " n = " + n));
//            }
        }
    }
}
