package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AirportFlightPartitioner extends Partitioner<CompositeKeyComparable, Text> {

    @Override
    public int getPartition(CompositeKeyComparable key, Text value, int numTasks) {
        return key.getAirportId() % numTasks;
    }
}
