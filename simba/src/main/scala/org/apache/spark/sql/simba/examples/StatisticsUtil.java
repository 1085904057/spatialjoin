package org.apache.spark.sql.simba.examples;

import java.util.Collections;

public class StatisticsUtil {
    public static Long max(java.util.ArrayList<Long> rawData) {
        return rawData.stream().max(Long::compareTo).orElse(0L);
    }

    public static Long min(java.util.ArrayList<Long> rawData) {
        return rawData.stream().min(Long::compareTo).orElse(0L);
    }

    public static Double average(java.util.ArrayList<Long> rawData) {
        return rawData.stream().mapToLong(o -> o).average().orElse(0.0);
    }

    public static Long median(java.util.ArrayList<Long> rawData) {
        if (rawData.size() == 0) {
            return 0L;
        }
        Collections.sort(rawData);
        if (rawData.size() % 2 == 0) {
            return (rawData.get((rawData.size() >> 1) - 1) + rawData.get(rawData.size() >> 1)) >> 1;
        } else {
            return rawData.get(rawData.size() >> 1);
        }
    }
}
