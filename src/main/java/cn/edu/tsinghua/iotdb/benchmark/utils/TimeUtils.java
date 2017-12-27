package cn.edu.tsinghua.iotdb.benchmark.utils;

import org.joda.time.DateTime;

public class TimeUtils {
	public static long generateTimestamp(long startTime, long interval, long index){
		return startTime + interval * index;
	}
	
	public static long convertDateStrToTimestamp(String dateStr){
		DateTime dateTime = new DateTime(dateStr);
		return dateTime.getMillis();
	}
	
	public static void main(String[] args) {
		System.out.println(convertDateStrToTimestamp("2006-01-26T13:30:00+08:00"));
		System.out.println(convertDateStrToTimestamp("2006-01-26T13:30:01+08:00"));
	}

}
