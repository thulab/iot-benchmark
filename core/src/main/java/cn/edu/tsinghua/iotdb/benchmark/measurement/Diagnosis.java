package cn.edu.tsinghua.iotdb.benchmark.measurement;

import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.DiagnosisItem;
import java.util.EnumMap;
import java.util.Map;

public class Diagnosis {

    private final Map<DiagnosisItem, Long> timeCost = new EnumMap<>(DiagnosisItem.class);
    private final Map<DiagnosisItem, Integer> count = new EnumMap<>(DiagnosisItem.class);

    private Diagnosis() {
        for(DiagnosisItem diagnosisItem: DiagnosisItem.values()){
            timeCost.put(diagnosisItem, 0L);
            count.put(diagnosisItem, 0);
        }
    }

    public void add(DiagnosisItem key, long value) {
        timeCost.put(key, timeCost.get(key) + value);
        count.put(key, count.get(key) + 1);
    }

    public void show() {
        long total = 0;
        for(DiagnosisItem diagnosisItem: DiagnosisItem.values()){
            total += timeCost.get(diagnosisItem);
        }
        if (total > 0) {
            for (DiagnosisItem diagnosisItem : DiagnosisItem.values()) {
                double p = (double) timeCost.get(diagnosisItem) / (double) total;
                double d = (double) timeCost.get(diagnosisItem) / count.get(diagnosisItem) / 1000000D;
                System.out.println(diagnosisItem + " cost " + String.format("%.2f", p * 100) + "% of test elapsed time. Average cost " + d + " ms. count:" + count.get(diagnosisItem));
            }
            System.out.println("Total diagnosisItem time: " + (total / 1000000D) + " ms");
        }
    }

    private static class DiagnosisHolder {
        private static final Diagnosis INSTANCE = new Diagnosis();
    }

    public static Diagnosis getInstance() {
        return DiagnosisHolder.INSTANCE;
    }

}
