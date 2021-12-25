package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKeyComparable implements WritableComparable<CompositeKeyComparable> {
    private IntWritable airportId;
    private IntWritable type;

    public CompositeKeyComparable() {
        airportId = new IntWritable(-1);
        type = new IntWritable(-1);
    }

    public CompositeKeyComparable(int id, int tableType) {
        airportId = new IntWritable(id);
        type = new IntWritable(tableType);
    }

    @Override
    public int compareTo(CompositeKeyComparable o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
