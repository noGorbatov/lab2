package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class TableJoinerApp {

    public static void main(String[] args) throws
            IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.println("Usage:");
            System.exit(-1);
        }
        Job job = Job.getInstance();

    }
}
