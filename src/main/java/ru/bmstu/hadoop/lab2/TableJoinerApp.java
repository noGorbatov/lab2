package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

public class TableJoinerApp {

    public static void main(String[] args) throws
            IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.println("Usage:");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        Path airportPath = new Path(args[0]);
        Path flightsPath = new Path(args[1]);
        Path outPath = new Path(args[2]);
        job.setJarByClass(TableJoinerApp.class);
        MultipleInputs.addInputPath(job, , TextInputFormat.class, AirportMapper.class);
        MultipleInputs
    }
}
