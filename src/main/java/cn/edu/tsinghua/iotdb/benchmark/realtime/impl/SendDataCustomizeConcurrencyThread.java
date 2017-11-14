package com.k2data.datatool.realtime.impl;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.k2data.common.utils.SleepUtils;
import com.k2data.datatool.utils.LogUtils;
import com.k2data.platform.ddm.sdk.builder.KMXRecord;
import com.k2data.platform.ddm.sdk.client.KMXClient;

import k2data.kmx.utils.dynamicdata.DataGenerateUtils;
import k2data.kmx.utils.dynamicdata.SendDataUtils;
import k2data.kmx.utils.metadata.MetaDataFileUtils;

public class SendDataCustomizeConcurrencyThread extends Thread {
	static Logger logger = Logger.getLogger(SendDataCustomizeConcurrencyThread.class);
	public volatile boolean isRunning = true;
	String measurement;
	String metadataUid;
	String sendDataUid;
	String logLocation;
	KMXClient client;
	boolean useCurrentTime;
	long startTime;
	long dataFreq;
	boolean isSendFromRest;
	boolean isJSONRecord;
	long sendDataInterval;
	long endTime;
	String sendToUrl;
	String sendFrom;
	String devSenRelationFile;
	boolean haveFgId;
	HashMap<String, Long> statistics;
	boolean needDetailsLog;

	/*
	 * init parameters
	 */
	public SendDataCustomizeConcurrencyThread(String measurement, String metadataUid, String sendDataUid,
			String logLocation, KMXClient client, boolean useCurrentTime, long startTime, long dataFreq,
			boolean isSendFromRest, boolean isJSONRecord, long sendDataInterval, long endTime, String sendToUrl,
			String sendFrom, String devSenRelationFile, boolean haveFgId, HashMap<String, Long> statistics,
			boolean needDetailsLog) {
		super();
		this.measurement = measurement;
		this.metadataUid = metadataUid;
		this.sendDataUid = sendDataUid;
		this.logLocation = logLocation;
		this.client = client;
		this.useCurrentTime = useCurrentTime;
		this.startTime = startTime;
		this.dataFreq = dataFreq;
		this.isSendFromRest = isSendFromRest;
		this.isJSONRecord = isJSONRecord;
		this.sendDataInterval = sendDataInterval;
		this.endTime = endTime;
		this.sendToUrl = sendToUrl;
		this.sendFrom = sendFrom;
		this.devSenRelationFile = devSenRelationFile;
		this.haveFgId = haveFgId;
		this.statistics = statistics;
		this.needDetailsLog = needDetailsLog;
	}

	public void close() {
		this.isRunning = false;
	}

	public void run() {
		long sampleTime = System.currentTimeMillis();
		if (!useCurrentTime) {
			sampleTime = startTime;
		}
		Long threadStart = System.currentTimeMillis();
		long successNum = 0;
		long errorNum = 0;
		long sendNum = 0;
		long reTryNum = 0;
		long payloadSize = 0;
		long totalPayloadSize = 0;
		long logCost = 0;
		long sendEt = 0;
		long all_genDataCost = 0;
		long all_sendDataCost = 0;
		int code = 999;
		long cost = 0;
		String reason = "Unexpected error";
		String postDataUrl = sendToUrl + "/channels/devices/data";
		long lastSampleTime = 0;
		String threadName = Thread.currentThread().getName();
		logger.info(Thread.currentThread().getName() + " start to send data...");
		HashMap<String, JSONObject> allIdFields = MetaDataFileUtils.getAllIdFiledsForAssets(devSenRelationFile);
		HashMap<String, JSONObject> allNonIdFields = MetaDataFileUtils.getAllNoneFiledsForAssets(devSenRelationFile);
		HashMap<String, String> sysIdFgIdMap = MetaDataFileUtils.getAssetSysIdAndFGIdMapping(devSenRelationFile);
		while (isRunning && (sampleTime <= endTime)) {

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
			for (Entry e : allIdFields.entrySet()) {
				if (!isRunning) {
					break;
				}
				String restPayload = "";
				KMXRecord record = null;
				long _sampleTime = System.currentTimeMillis();
				String assetSysId = e.getKey().toString();
				String fgId = sysIdFgIdMap.get(assetSysId);
				JSONObject idFields = (JSONObject) e.getValue();
				JSONObject nonIdFields = (JSONObject) allNonIdFields.get(assetSysId);
				if (!haveFgId) {
					fgId = "empty";
				}
				long genDataSt = System.currentTimeMillis();
				if (isSendFromRest) {
					restPayload = DataGenerateUtils.genDynamicDataRestPayload(fgId, idFields, nonIdFields, sampleTime);
					payloadSize = restPayload.length();
				} else {
					record = DataGenerateUtils.genDynamicDataKMXSDKRecord(isJSONRecord, fgId, idFields, nonIdFields,
							sampleTime);
					payloadSize = record.toKafkaProducerRecord().value().toString().length();

				}
				long genDataEt = System.currentTimeMillis();
				long genDataCost = genDataEt - genDataSt;
				all_genDataCost = all_genDataCost + genDataCost;

				if (sendDataInterval > ((genDataEt - _sampleTime) + logCost + cost)) {
					SleepUtils.sleep(sendDataInterval - (genDataEt - _sampleTime) - logCost - cost);
				}
				/*
				 * send data here
				 */

				sendNum++;
				statistics.put("sent", sendNum);
				HashMap<String, Object> currentSent = new HashMap<String, Object>();
				if (isSendFromRest) {
					currentSent = SendDataUtils.sendRecordFromRest(postDataUrl, restPayload);
				} else {
					currentSent = SendDataUtils.sendRecordByKMXSDK(client, record);
				}
				sendEt = System.currentTimeMillis();
				code = Integer.parseInt(currentSent.get("rc").toString());
				reason = currentSent.get("reason").toString();
				cost = Long.parseLong(currentSent.get("sendCost").toString());
				all_sendDataCost = all_sendDataCost + cost;
				switch (code) {
				case 202:
					successNum++;
					totalPayloadSize = totalPayloadSize + payloadSize;
					break;
				default:
					errorNum++;
					break;
				}
				if (needDetailsLog) {
					LogUtils.logSendDataDetailsToFile(logLocation, measurement, metadataUid, sendDataUid, threadName,
							fgId, assetSysId, _sampleTime, code, reason, payloadSize, cost, genDataCost, sendFrom);
				}
				statistics.put("success", successNum);
				statistics.put("fail", errorNum);
				statistics.put("sendDataCost", all_sendDataCost);
				statistics.put("genDataCost", all_genDataCost);
				statistics.put("payloadSize", totalPayloadSize);
				logCost = System.currentTimeMillis() - sendEt;
			}
			if (!useCurrentTime) {
				sampleTime = sampleTime + dataFreq;
			}
		}
		LogUtils.LogSummayToFile(logLocation, Thread.currentThread().getName(), sendNum, successNum, errorNum, reTryNum,
				System.currentTimeMillis() - threadStart, totalPayloadSize);
		System.out.println("Thread: " + Thread.currentThread().getName() + " stopped.");
	}
}
