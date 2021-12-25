package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKeyComparable implements WritableComparable<CompositeKeyComparable> {
    private IntWritable airportId;
    private IntWritable tableType;
    static public final int AIRPORT_KEY = 0;
    static public final int FLIGHT_KEY = 1;

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
        int cmp = airportId.compareTo(key.airportId);  //TODO: возможно поменять приоритет
        if (cmp != 0) {
            return cmp;
        }
        return tableType.compareTo(key.tableType);
//        return ;
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
