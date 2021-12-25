package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

public class TableJoinerApp {
    public static void main(String[] args) throws
            IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 3) {
            System.err.println("Usage: tablejoinerapp <airports table> <flights table> <output dir>");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        Path airportsPath = new Path(args[0]);
        Path flightsPath = new Path(args[1]);
        Path outPath = new Path(args[2]);
        job.setJarByClass(TableJoinerApp.class);
        MultipleInputs.addInputPath(job, airportsPath, TextInputFormat.class, AirportMapper.class);
        MultipleInputs.addInputPath(job, flightsPath, TextInputFormat.class, FlightMapper.class);
        job.setInputKeyClass
        FileOutputFormat.setOutputPath(job, outPath);
        job.setReducerClass(JoinTableReducer.class);
        job.setNumReduceTasks(2);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
