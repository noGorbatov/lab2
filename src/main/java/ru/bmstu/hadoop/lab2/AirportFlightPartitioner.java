package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AirportFlightPartitioner extends Partitioner<CompositeKeyComparable, Text> {

    @Override
    public int getPartition(CompositeKeyComparable compositeKeyComparable, Text text, int i) {
        return 0;
    }
}
