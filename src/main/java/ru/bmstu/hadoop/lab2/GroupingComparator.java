package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {
    @Override
    public int compare(WritableComparable o1, WritableComparable o2) {
        CompositeKeyComparable k1 = (CompositeKeyComparable) o1;
        CompositeKeyComparable k2 = (CompositeKeyComparable) o2;

        if (k1.getAirportId())
    }
}