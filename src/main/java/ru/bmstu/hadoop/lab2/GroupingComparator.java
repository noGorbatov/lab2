package ru.bmstu.hadoop.lab2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {
    protected GroupingComparator() {
        super(CompositeKeyComparable.class, true);
    }
    @Override
    public int compare(WritableComparable o1, WritableComparable o2) {
        CompositeKeyComparable k1 = (CompositeKeyComparable) o1;
        CompositeKeyComparable k2 = (CompositeKeyComparable) o2;

        return k1.getAirportId() - k2.getAirportId();
    }
}
