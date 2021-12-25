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
        int id1 = k1.getAirportId();
        int id2 = k2.getAirportId();
        return Integer.compare(id1, id2);
//        return -1;
    }
}
