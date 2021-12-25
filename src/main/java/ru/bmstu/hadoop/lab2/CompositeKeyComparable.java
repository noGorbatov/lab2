package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKeyComparable implements WritableComparable<CompositeKeyComparable> {
    private IntWritable airportId;
    private IntWritable tableType;

    public CompositeKeyComparable() {
        airportId = new IntWritable(-1);
        tableType = new IntWritable(-1);
    }

    public CompositeKeyComparable(int id, int type) {
        airportId = new IntWritable(id);
        tableType = new IntWritable(type);
    }

    @Override
    public int compareTo(CompositeKeyComparable key) {
        int cmp = tableType.compareTo(key.tableType);
        if (cmp != 0) {
            return cmp;
        }
        return airportId.compareTo(key.airportId);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        airportId.write(dataOutput);
        tableType.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        airportId.readFields(dataInput);
        tableType.readFields(dataInput);
    }

    public int getAirportId() {
        return airportId.get();
    }

    public int getTableType() {
        return tableType.get();
    }
}
