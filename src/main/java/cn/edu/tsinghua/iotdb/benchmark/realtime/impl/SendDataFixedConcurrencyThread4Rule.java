package com.k2data.datatool.realtime.impl;

import java.util.HashMap;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.k2data.common.utils.SleepUtils;
import com.k2data.datatool.utils.LogUtils;
import com.k2data.platform.ddm.sdk.builder.KMXRecord;
import com.k2data.platform.ddm.sdk.client.KMXClient;

import k2data.kmx.utils.dynamicdata.DataGenerateUtils;
import k2data.kmx.utils.dynamicdata.SendDataUtils;

public class SendDataFixedConcurrencyThread4Rule extends Thread {
	static Logger logger = Logger.getLogger(SendDataFixedConcurrencyThread4Rule.class);
	public volatile boolean isRunning = true;
	boolean useCurrentTime;
	String measurement;
	String metadataUid;
	String sendDataUid;
	String logLocation;
	boolean isSendFromRest;
	boolean isJSONRecord;
	long startTime;
	long endTime;
	long sendDataInterval;
	long dataFreq;
	KMXClient client;
	String url;
	String ip;
	String fgId;
	String assetId;
	JSONObject idFields;
	JSONObject nonIdFields;
	boolean haveFgId;
	HashMap<String, Long> statistics;
	boolean needDetailLog;
	
	private int wrongRate = 0;

	public void close() {
		this.isRunning = false;
	}

	public SendDataFixedConcurrencyThread4Rule(String measurement, String metadataUid, String sendDataUid, String logLocation,
			boolean isSendFromRest, boolean isJSONRecord, boolean useCurrentTime, long startTime, long endTime,
			long sendDataInterval, long dataFreq, KMXClient client, String url, String ip, String fgId, String assetId,
			JSONObject idFields, JSONObject nonIdFields, boolean haveFgId, HashMap<String, Long> statistics,
			boolean needDetailLog, int wrongRate) {

		this.measurement = measurement;
		this.metadataUid = metadataUid;
		this.sendDataUid = sendDataUid;
		this.logLocation = logLocation;
		this.isSendFromRest = isSendFromRest;
		this.isJSONRecord = isJSONRecord;
		this.useCurrentTime = useCurrentTime;
		this.startTime = startTime;
		this.endTime = endTime;
		this.sendDataInterval = sendDataInterval;
		this.dataFreq = dataFreq;
		this.client = client;
		this.url = url;
		this.ip = ip;
		this.fgId = fgId;
		this.assetId = assetId;
		this.idFields = idFields;
		this.nonIdFields = nonIdFields;
		this.haveFgId = haveFgId;
		this.statistics = statistics;
		this.needDetailLog = needDetailLog;
		this.wrongRate = wrongRate;
	}

	public void run() {
		long sampleTime = System.currentTimeMillis();
		if (!useCurrentTime) {
			sampleTime = startTime;
		}
		long successNum = 0;
		long errorNum = 0;
		long sendNum = 0;
		long reTryNum = 0;
		long payloadSize = 0;
		long totalSize = 0;
		long totalCost = 0;
		long logCost = 0;
		long all_genDataCost = 0;
		long all_sendDataCost = 0;
		int code = 999;
		long cost = 0;
		String reason = "unexpected error";
		long lastSampleTime = 0;
		// post from default channel
		String threadName = Thread.currentThread().getName();
		logger.info(threadName + " start to send data...");
		String postDataUrl = url + "/channels/devices/data";
		while (isRunning && (sampleTime <= endTime)) {

			String restPayload = "";
			KMXRecord record = null;
			if (useCurrentTime) {
				sampleTime = System.currentTimeMillis();
				/*
				 * to avoid duplicate data
				 */
				if (lastSampleTime == sampleTime) {
					sampleTime = sampleTime + 1;
				}
				lastSampleTime = sampleTime;
			}
			long _sampleTime = System.currentTimeMillis();
			if (!haveFgId) {
				fgId = "empty";
			}
			
			boolean wrong;
			if(wrongRate != 0){
				wrong = false;
				if(sendNum % wrongRate == 0){
					wrong = true;
				}else{
					wrong = false;
				}
			}else{
				wrong = true;
			}
			
			if (isSendFromRest) {	
				restPayload = DataGenerateUtils.genDynamicDataRestPayload4Rule(fgId, idFields, nonIdFields, sampleTime,wrong);
				System.out.println(restPayload);
				payloadSize = restPayload.length();
			} else {				
				record = DataGenerateUtils.genDynamicDataKMXSDKRecord4Rule(isJSONRecord, fgId, idFields, nonIdFields,
						sampleTime,wrong);
				payloadSize = record.toKafkaProducerRecord().value().toString().length();

			}
			long genDataCost = System.currentTimeMillis() - _sampleTime;
			all_genDataCost = all_genDataCost + genDataCost;

			long et = System.currentTimeMillis();
			if (sendDataInterval > ((et - _sampleTime) + logCost + cost)) {
				SleepUtils.sleep(sendDataInterval - (et - _sampleTime) - logCost - cost);
			}
			/*
			 * send data here
			 */
			sendNum++;
			HashMap<String, Object> currentSent = new HashMap<String, Object>();
			if (isSendFromRest) {
				currentSent = SendDataUtils.sendRecordFromRest(postDataUrl, restPayload);
			} else {
				currentSent = SendDataUtils.sendRecordByKMXSDK(client, record);
			}
			long sendEt = System.currentTimeMillis();
			code = Integer.parseInt(currentSent.get("rc").toString());
			reason = currentSent.get("reason").toString();
			cost = Long.parseLong(currentSent.get("sendCost").toString());
			all_sendDataCost = all_sendDataCost + cost;
			switch (code) {
			case 202:
				successNum++;
				totalSize = totalSize + payloadSize;
				break;
			default:
				errorNum++;
				break;
			}
			if (needDetailLog) {
				LogUtils.logSendDataDetailsToFile(logLocation, measurement, metadataUid, sendDataUid, threadName, fgId,
						assetId, _sampleTime, code, reason, payloadSize, cost, genDataCost, ip);
			}
			statistics.put("sent", sendNum);
			statistics.put("fail", errorNum);
			statistics.put("success", successNum);
			statistics.put("sendDataCost", all_sendDataCost);
			statistics.put("genDataCost", all_genDataCost);
			statistics.put("payloadSize", totalSize);
			if (!useCurrentTime) {
				sampleTime = sampleTime + dataFreq;
			}
			logCost = System.currentTimeMillis() - sendEt;

		}

		LogUtils.LogSummayToFile(logLocation, assetId, sendNum, successNum, errorNum, reTryNum, totalCost, totalSize);
		logger.info("Thread: " + fgId + " stopped.");
	}
}
