package com.k2data.datatool.realtime;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.k2data.common.utils.DateUtils;
import com.k2data.common.utils.EnvUtils;
import com.k2data.common.utils.JsonUtils;
import com.k2data.common.utils.K2FileUtils;
import com.k2data.common.utils.SleepUtils;
import com.k2data.configs.Configuration;
import com.k2data.datatool.realtime.impl.SendDataCustomizeConcurrencyThread;
import com.k2data.datatool.realtime.impl.SendDataFixedConcurrencyThread;
import com.k2data.datatool.utils.ImpalaResultCheck;
import com.k2data.datatool.utils.LogUtils;
import com.k2data.platform.ddm.sdk.client.KMXClient;
import com.k2data.platform.ddm.sdk.client.KMXConfig;
import com.k2data.platform.ddm.sdk.common.DataType;
import com.k2data.platform.ddm.sdk.common.ParamNames;
import com.k2platform.models.qatools.controls.realTime.RealTimeDataToolControl;
import com.k2platform.models.qatools.results.realtime.RealTimeDataToolReport;
import com.k2platform.models.qatools.results.realtime.RealTimeDataToolResults;
import com.k2platform.models.qatools.results.realtime.RealTimeDetailsResult;
import com.k2platform.models.qatools.results.realtime.RealTimeSummaryResult;
import com.k2platform.models.qatools.results.realtime.RealTimeToolMetaResult;

import k2data.kmx.utils.metadata.MetaDataFileUtils;

public class RealTimeDataClient {

	static Logger logger = Logger.getLogger(RealTimeDataClient.class);

	public RealTimeDetailsResult sendDataClient(RealTimeDataToolControl control) {

		boolean isSendFromRest = control.getRealTimeMeta().getDataSendBy();
		String sentTo = control.getRealTimeMeta().getDataSentTo();
		boolean isJson = control.getRealTimeMeta().getRecordType();
		Integer producerNum = control.getRealTimeMeta().getKmxClientNum();
		boolean useCurrentTime = control.getRealTimeMeta().getGenerateDataMethod();
		Long startTime = control.getDynamicDataGen().getDataStartTime();
		long duration = control.getDynamicDataGen().getDataDuration();
		int concurrency = control.getRealTimeMeta().getConcurrencyNum();
		long dataFreq = control.getDynamicDataGen().getDataFreq();
		long sendDataInterval = control.getRealTimeMeta().getSentDataInterval();
		boolean haveFgId = control.getRealTimeMeta().getIsHaveFieldGroupId();
		boolean needDetailsLog = control.getRealTimeMeta().getIsNeedDetailsLog();
		String measurement = control.getRealTimeMeta().getMeasurement();
		String ip = EnvUtils.getK2Ip();
		ArrayList<HashMap<String, Long>> all_statistics = new ArrayList<HashMap<String, Long>>();
		ArrayList<SendDataCustomizeConcurrencyThread> clients = new ArrayList<SendDataCustomizeConcurrencyThread>();
		ArrayList<SendDataFixedConcurrencyThread> clients_dev = new ArrayList<SendDataFixedConcurrencyThread>();
		ArrayList<KMXClient> kmxClients = new ArrayList<KMXClient>();
		String dataPath = K2FileUtils.filePath(control.getSdmCheckerControl().getTestEnvConfigModel().getOutputDir());
		String deviceSensorRelationFilePath = dataPath + "templates/device_sensor_relation";
		String logLocation = K2FileUtils.filePath(
				control.getSdmCheckerControl().getTestEnvConfigModel().getOutputDir() + "realTime") + "sendDataLogs";
		String metadataUid = MetaDataFileUtils.getMetaDataUID(dataPath + "templates/metadataInfo");
		String sendDataUid = DateUtils.toDateTime(System.currentTimeMillis()) + "_" + metadataUid;
		boolean threadFlag = true;
		if (!new File(logLocation).exists()) {

			K2FileUtils.backupAndRebuildDir(logLocation);
		}
		K2FileUtils.createFile(logLocation, "running");
		int assetNum = MetaDataFileUtils.getAssetNum(deviceSensorRelationFilePath);
		int fieldNumPerFG = MetaDataFileUtils.getFieldsNumPerFG(deviceSensorRelationFilePath);

		KMXConfig config = new KMXConfig();
		if (!isSendFromRest) {
			config.put(ParamNames.PLATFORM_SERVER, sentTo);
			if (isJson) {
				config.put(ParamNames.DATA_TYPE, DataType.JSON_STRING);
			} else {
				config.put(ParamNames.DATA_TYPE, DataType.BINARY);
			}
			for (int i = 0; i < producerNum; i++) {
				KMXClient client = new KMXClient(config);
				kmxClients.add(client);
			}

		} else {

			producerNum = 0;
		}
		long jobStart = System.currentTimeMillis();

		if (useCurrentTime) {
			startTime = jobStart;
		}
		long endTime = startTime + duration;
		if (assetNum > concurrency) {
			String splitedDevSenFiledDir = dataPath + "templates/splitFiles";
			MetaDataFileUtils.splitDevSenRelationFileByConcurrency(deviceSensorRelationFilePath, concurrency,
					splitedDevSenFiledDir);
			File[] splitedFileList = new File(splitedDevSenFiledDir).listFiles();
			for (int j = 0; j < concurrency; j++) {
				HashMap<String, Long> statistics = new HashMap<String, Long>();
				KMXClient kc = null;
				if (!kmxClients.isEmpty()) {
					kc = kmxClients.get(j % producerNum);
				}

				SendDataCustomizeConcurrencyThread client = new SendDataCustomizeConcurrencyThread(measurement,
						metadataUid, sendDataUid, logLocation, kc, useCurrentTime, startTime, dataFreq, isSendFromRest,
						isJson, sendDataInterval, endTime, sentTo, ip, splitedFileList[j].getAbsolutePath(), haveFgId,
						statistics, needDetailsLog);
				client.start();
				all_statistics.add(statistics);
				clients.add(client);
			}
		} else {
			logger.info("Since assets num less than concurrency ,so use devices num as concurrency.");
			threadFlag = false;
			concurrency = assetNum;
			HashMap<String, JSONObject> allIdFields = MetaDataFileUtils
					.getAllIdFiledsForAssets(deviceSensorRelationFilePath);
			HashMap<String, JSONObject> allNonIdFields = MetaDataFileUtils
					.getAllNoneFiledsForAssets(deviceSensorRelationFilePath);
			HashMap<String, String> sysIdFgIdMap = MetaDataFileUtils
					.getAssetSysIdAndFGIdMapping(deviceSensorRelationFilePath);
			int num = 0;
			
			for (Entry e : allIdFields.entrySet()) {
				KMXClient kc = null;
				if(!kmxClients.isEmpty()){
					kc= kmxClients.get(num % producerNum);
				}
				HashMap<String, Long> statistics = new HashMap<String, Long>();
				String assetId = e.getKey().toString();
				String fgId = sysIdFgIdMap.get(assetId);
				JSONObject idFields = (JSONObject) e.getValue();
				JSONObject nonIdFields = (JSONObject) allNonIdFields.get(assetId);
				SendDataFixedConcurrencyThread client = new SendDataFixedConcurrencyThread(measurement, metadataUid,
						sendDataUid, logLocation, isSendFromRest, isJson, useCurrentTime, startTime, endTime,
						sendDataInterval, dataFreq,kc, sentTo, ip, fgId, assetId,
						idFields, nonIdFields, haveFgId, statistics, needDetailsLog);

				client.start();
				all_statistics.add(statistics);
				clients_dev.add(client);
				num++;
			}
		}
		int checker = 0;
		while (true) {

			if (threadFlag) {
				if (isAllThreadTerminated(clients)) {
					break;
				}

			} else {
				if (isAllThreadTerminated_dev(clients_dev)) {
					break;
				}
			}

			if (!(new File(K2FileUtils.filePath(logLocation) + "running").exists())) {
				logger.info("Start to stopping thread");

				if (threadFlag) {
					for (int clientSize = 0; clientSize < clients.size(); clientSize++) {
						if (clients.get(clientSize).getState() != Thread.State.TERMINATED) {
							clients.get(clientSize).close();
						}
					}
				} else {

					for (int clientSize = 0; clientSize < clients_dev.size(); clientSize++) {
						if (clients_dev.get(clientSize).getState() != Thread.State.TERMINATED) {
							clients_dev.get(clientSize).close();
						}
					}
				}
				while (!clients.isEmpty() || !clients_dev.isEmpty()) {
					if (threadFlag) {
						for (int clientSize = 0; clientSize < clients.size(); clientSize++) {
							if (Thread.State.TERMINATED == clients.get(clientSize).getState()) {
								clients.remove(clientSize);
							}
						}
					} else {
						for (int clientSize = 0; clientSize < clients_dev.size(); clientSize++) {
							if (Thread.State.TERMINATED == clients_dev.get(clientSize).getState()) {
								clients_dev.remove(clientSize);
							}
						}
					}
				}
				logger.info("All threads stopped");

				endTime = System.currentTimeMillis();
				break;
			}
			checker++;
			if (checker % 60 == 0) {
				logger.info("I am still working ...");
			}
			if (control.getRealTimeMeta().getIsNeedTempResult()) {
				if (checker % 300 == 0) {
					long left = endTime - System.currentTimeMillis();
					LogUtils.logTempResultToFile(logLocation, duration - left, left, all_statistics, fieldNumPerFG);
				}
			}
			SleepUtils.sleep(1000);
		}

		long jobEndTime = System.currentTimeMillis();

		if (useCurrentTime) {
			startTime = jobStart;
			endTime = jobEndTime;
		}

		RealTimeDetailsResult details = LogUtils.statisticsData(all_statistics);

		details.setMetaDataUid(metadataUid);
		details.setDynamicDataUid(sendDataUid);
		details.setJobStartTime(jobStart);
		details.setJobEndTime(jobEndTime);
		details.setReportTime(System.currentTimeMillis());
		details.setDataStartTime(startTime);
		details.setDataEndTime(endTime);
		K2FileUtils.removeFile(K2FileUtils.filePath(logLocation), "running");
		return details;

	}

	/**
	 * 
	 * @param clients
	 * @return
	 */
	public static boolean isAllThreadTerminated(ArrayList<SendDataCustomizeConcurrencyThread> clients) {

		boolean status = true;

		for (SendDataCustomizeConcurrencyThread thread : clients) {

			status = status && (thread.getState() == Thread.State.TERMINATED);

		}
		return status;

	}

	/**
	 * 
	 * @param clients
	 * @return
	 */
	public static boolean isAllThreadTerminated_dev(ArrayList<SendDataFixedConcurrencyThread> clients) {
		boolean status = true;
		for (SendDataFixedConcurrencyThread thread : clients) {
			status = status && (thread.getState() == Thread.State.TERMINATED);
		}
		return status;
	}

	public static RealTimeDataToolReport sendData(RealTimeDataToolControl control) {

		String rootOutPutDir = control.getSdmCheckerControl().getTestEnvConfigModel().getOutputDir();
		String realtimeOutPutDir = rootOutPutDir + "realTime";
		String sendDataLogDir = realtimeOutPutDir + "/sendDataLogs";
		String templateDir = rootOutPutDir + "templates";
		String templateRelationFile = templateDir + "/device_sensor_relation";
		logger.info("backup log dir");
		K2FileUtils.backupAndRebuildDir(sendDataLogDir);
		logger.info("create send data details log dir");
		// K2FileUtils.mkdirs(sendDataLogDir + "/influx");
		K2FileUtils.deleteAndRebuildDir(K2FileUtils.filePath(templateDir) + "/splitFiles");
		SleepUtils.sleep(2000);
		RealTimeDataClient client = new RealTimeDataClient();
		//

		RealTimeDetailsResult sentDataResult = client.sendDataClient(control);

		logger.info("start to generate report");
		try {
			logger.info(JsonUtils.prettyWithNull(sentDataResult));
		} catch (Exception e) {
			e.printStackTrace();
		}

		JSONObject k2dbResult = new JSONObject();
		int checkNum = 5;

		for (int i = 0; i < checkNum; i++) {

			logger.info("Check if all data loaded into K2DB..");
			if (i == 0) {
				logger.info("Sleep 10 mins to wait Data processing..");
				SleepUtils.sleep(600000);
			}
			k2dbResult = ImpalaResultCheck.checkImpalaRowCountAsExpected(templateRelationFile,
					control.getSdmCheckerControl().getKmxEnvControl().getImpalaIp(),
					DateUtils.toISO8(sentDataResult.getDataStartTime()),
					DateUtils.toISO8(sentDataResult.getDataEndTime()), sentDataResult.getAllSuccess());
			if (k2dbResult.getBooleanValue("status")) {
				break;
			} else {
				logger.info("Check row count in K2DB fail, wait 10 mins re-check");
				SleepUtils.sleep(600000);
			}
		}
		sentDataResult.setK2dbTotalCount(k2dbResult.getLong("all"));
		sentDataResult.setK2dbDistinctCount(k2dbResult.getLong("distinct"));

		RealTimeDataToolReport report = new RealTimeDataToolReport();
		RealTimeToolMetaResult meta = new RealTimeToolMetaResult();
		RealTimeSummaryResult summary = new RealTimeSummaryResult();
		RealTimeDataToolResults results = new RealTimeDataToolResults();

		summary.setLoadToImpalaStatus(k2dbResult.getBooleanValue("status"));
		summary.setSendDataStatus(
				sentDataResult.getAllSent() == sentDataResult.getAllSuccess() && sentDataResult.getAllSent() > 0);
		meta.setEnv(control.getSdmCheckerControl().getKmxEnvControl());
		meta.setSdmMeta(control.getSdmCheckerControl().getSdmMetaControl());
		meta.setRealTimeMeta(control.getRealTimeMeta());
		results.setDetails(sentDataResult);
		results.setSummary(summary);
		report.setMeta(meta);
		report.setResults(results);
		System.out.println(JSON.toJSONString(report, SerializerFeature.PrettyFormat));
		try {
			K2FileUtils.writeContentsToFile(K2FileUtils.filePath(sendDataLogDir) + "Report.txt",
					JsonUtils.prettyWithNull(JSON.parseObject(JSON.toJSONString(report))), false);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return report;

	}

	public static void main(String[] args) {

		Configuration config = new Configuration();
		sendData(new RealTimeDataToolControl(config));

	}
}
