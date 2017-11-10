package cn.edu.tsinghua.iotdb.benchmark.loadData;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;

import java.util.ArrayList;
import java.util.List;

public class Point {
	public String measurement;
	public List<String> tagName = new ArrayList<String>();
	public List<String> tagValue = new ArrayList<String>();
	public List<String> fieldName = new ArrayList<String>();
	public List<Number> fieldValue = new ArrayList<Number>();
	public long time;
	private Config config =  ConfigDescriptor.getInstance().getConfig();
	
	public String creatInsertStatement(){
		StringBuilder builder = new StringBuilder();
		if(config.TAG_PATH) {
			builder.append("insert into ").append(getPath()).append("(timestamp");
		}else {
			builder.append("insert into ").append("root.device_0").append("(timestamp");
		}
		for(String sensor: fieldName){
			builder.append(",").append(sensor);
		}
		builder.append(") values(");
		builder.append(time);
		
		for(Number sensorValue: fieldValue){
			builder.append(",").append(sensorValue);
		}
		builder.append(")");
		return builder.toString();
	}
	
	public String getPath(){
		StringBuilder builder = new StringBuilder();
		builder.append("root.");
		builder.append(measurement);
		int len = tagName.size();
		for(int i = 0; i < len; i++){
			builder.append(".").append(tagName.get(i)).append("--").append(tagValue.get(i));
		}

		return builder.toString();	
	}

}
