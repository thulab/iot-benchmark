package cn.edu.tsinghua.iotdb.benchmark.model;

import java.util.HashMap;
import java.util.Map;

/**
 * example:
 * [{
 *     "name": "archive.file.tracked",
 *     "timestamp": 1349109376,
 *     "type": "long",
 *     "value": 123,
 *     "tags":{"host":"test"}
 * },
 * {
 *     "name": "archive.file.search",
 *     "timestamp": 999,
 *     "type": "double",
 *     "value": 32.1,
 *     "tags":{"host":"test"}
 * }]
 */

public class KairosDataModel {
    private static final long serialVersionUID = 1L;
    private String name; //full path like root.perform.group_0.d_0.s_0
    private long timestamp;
    private String type;
    private Object value;
    private Map<String,String> tags=new HashMap<String,String>();
    public String getName() {
        return name;
    }
    public void setName(String metric) {
        this.name = metric;
    }
    public long getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
    public Object getValue() {
        return value;
    }
    public void setValue(Object value) {
        this.value = value;
    }
    public Map<String, String> getTags() {
        return tags;
    }
    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }
}
