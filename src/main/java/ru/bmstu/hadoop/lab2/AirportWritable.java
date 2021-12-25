package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportWritable implements Writable {
    private Text name;
    public AirportWritable() {
        name = new Text();
    }

    public AirportWritable(Text airportName) {
        name = airportName;
    }

    public void set(Text airportName) {
        name = airportName;
    }
    
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        name.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        name.readFields(dataInput);
    }
}
