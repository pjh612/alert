package com.alert.core.comparator;

public class DefaultIdComparator implements IdComparator {
    @Override
    public int compare(String o1, String o2) {
        if (o1 == null && o2 == null) return 0;
        if (o1 == null) return -1;
        if (o2 == null) return 1;

        if (o1.length() != o2.length()) {
            return Integer.compare(o1.length(), o2.length());
        }
        return o1.compareTo(o2);
    }
}
