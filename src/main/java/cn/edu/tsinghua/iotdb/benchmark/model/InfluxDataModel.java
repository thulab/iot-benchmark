package cn.edu.tsinghua.iotdb.benchmark.model;

import javafx.util.Pair;

import java.util.*;

/**
 * Created by Administrator on 2017/11/16 0016.
 */
public class InfluxDataModel {
    public String measurement;
    public HashMap<String, String> tagSet;
    public HashMap<String, Number> fields;
    public long timestamp;

    public InfluxDataModel() {
        tagSet = new HashMap<>();
        fields = new HashMap<>();
    }

    @Override
    /**
     *  e.g. cpu,host=server03,region=useast load=15.4 1434055562000000000
     */
    public String toString() {
        StringBuilder builder = new StringBuilder(measurement);
        // attach tags
        if(tagSet.size() > 0) {
            // sort by key to make the sequence unique
             ArrayList<Map.Entry<String, String>> entries = new ArrayList<>();
             entries.addAll(tagSet.entrySet());
             entries.sort(new Comparator<Map.Entry<String, String>>() {
                 @Override
                 public int compare(Map.Entry<String, String> o1, Map.Entry<String, String> o2) {
                     return o1.getKey().compareTo(o2.getKey());
                 }
             });
             for(Map.Entry<String, String> entry : entries) {
                 builder.append(",");
                 builder.append(entry.getKey());
                 builder.append("=");
                 builder.append(entry.getValue());
             }
        }
        // attach fields
        builder.append(" ");
        ArrayList<Map.Entry<String, Number>> entries = new ArrayList<>();
        entries.addAll(fields.entrySet());
        entries.sort(new Comparator<Map.Entry<String, Number>>() {
            @Override
            public int compare(Map.Entry<String, Number> o1, Map.Entry<String, Number> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });
        boolean isFirstField = true;
        for(Map.Entry<String, Number> entry : entries) {
            if(isFirstField){
                isFirstField = false;
            } else {
                builder.append(",");
            }
            builder.append(entry.getKey());
            builder.append("=");
            builder.append(entry.getValue().toString());
        }
        // attach timestamp
        builder.append(" ");
        builder.append(timestamp);
        return builder.toString();
    }
}
