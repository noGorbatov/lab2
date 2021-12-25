package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKeyComparable implements WritableComparable<CompositeKeyComparable> {
    private int airportId;
    private int type;
    public CompositeKeyComparable() {
        airportId = 0;
        type = -1;
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
