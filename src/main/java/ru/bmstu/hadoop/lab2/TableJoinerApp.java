package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.MultipleInputs;
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
        job.setJarByClass(TableJoinerApp.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat, );
    }
}
