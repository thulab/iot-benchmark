package cn.edu.tsinghua.iotdb.benchmark.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class CTSDBMetricModel implements Serializable {
    private static final long serialVersionUID = 1L;
    private Map<String,String> tags=new HashMap<String,String>();
    private Map<String,String> fields=new HashMap<String,String>();

    public Map<String, String> getFields() {
        return fields;
    }

    public void setFields(Map<String, String> fields) {
        this.fields = fields;
    }

    public Map<String, String> getTags() {
        return tags;
    }
    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }
}
