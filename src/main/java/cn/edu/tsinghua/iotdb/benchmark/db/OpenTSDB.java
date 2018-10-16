package cn.edu.tsinghua.iotdb.benchmark.db;

import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import cn.edu.tsinghua.iotdb.benchmark.distribution.PossionDistribution;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.model.TSDBDataModel;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import cn.edu.tsinghua.iotdb.benchmark.utils.HttpRequest;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * http://opentsdb.net/docs/build/html/index.html
 * 
 * @author fasape
 *
 */
public class OpenTSDB extends TSDB implements IDatebase {
	private static final Logger LOGGER = LoggerFactory.getLogger(OpenTSDB.class);
	private String openUrl;
	private String queryUrl;
	private String writeUrl;
	private String metric = "root.perform.";
	private Config config;
	private MySqlLog mySql = new MySqlLog();;
	private long labID;
	private Map<String, LinkedList<TSDBDataModel>> dataMap = new HashMap<>();
	private Random sensorRandom = null;
	private Random timestampRandom;
	private ProbTool probTool;
	private int backScanTime = 24 ;

	public OpenTSDB(long labID) {
		mySql = new MySqlLog();
		this.labID = labID;
		config = ConfigDescriptor.getInstance().getConfig();
		sensorRandom = new Random(1 + config.QUERY_SEED);
		timestampRandom = new Random(2 + config.QUERY_SEED);
		probTool = new ProbTool();
		openUrl = config.DB_URL;
		writeUrl = openUrl + "/api/put?summary ";
		queryUrl = openUrl + "/api/query";
		mySql.initMysql(labID);
	}

	@Override
	public void init() throws SQLException {
		//OpenTSDB do not support delete old data very well
	}

	@Override
	public void insertOneBatch(String device, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount)
			throws SQLException {
		LinkedList<String> keys = new LinkedList<>();
		for (int i = 0; i < config.CACHE_NUM; i++) {
			String key = UUID.randomUUID().toString();
			dataMap.put(key, createDataModel(batchIndex, i, device));
			keys.add(key);
		}
		insertOneBatch(keys, batchIndex, totalTime, errorCount);
	}

	@Override
	public void insertOneBatch(LinkedList<String> keys, int batchIndex, ThreadLocal<Long> totalTime,
			ThreadLocal<Long> errorCount) throws SQLException {
		long startTime = 0, endTime = 0;
		String response = null;
		LinkedList<TSDBDataModel> models = new LinkedList<>();
		for (String key : keys) {
			models.addAll(dataMap.get(key));
			dataMap.remove(key);
		}
		String body = JSON.toJSONString(models);
		LOGGER.debug(body);
		try {
			startTime = System.nanoTime();
			response = HttpRequest.sendPost(writeUrl, body);
			endTime = System.nanoTime();
			LOGGER.debug("response: " + response);
			int errorNum = JSON.parseObject(response).getInteger("failed");
			errorCount.set(errorCount.get() + errorNum);
			LOGGER.info("{} execute ,{}, batch, it costs ,{},s, totalTime ,{},s, throughput ,{}, point/s",
					Thread.currentThread().getName(), batchIndex, (endTime - startTime) / 1000000000.0,
					((totalTime.get() + (endTime - startTime)) / 1000000000.0),
					((models.size() - errorNum) / (double) (endTime - startTime)) * 1000000000);
			totalTime.set(totalTime.get() + (endTime - startTime));
			mySql.saveInsertProcess(batchIndex, (endTime - startTime) / 1000000000.0, totalTime.get() / 1000000000.0, errorNum,
					config.REMARK);
		} catch (IOException e) {
			errorCount.set(errorCount.get() + models.size());
			LOGGER.error("Batch insert failed, the failed num is ,{}, Error：{}", models.size(), e.getMessage());
			mySql.saveInsertProcess(batchIndex, (endTime - startTime) / 1000000000.0, totalTime.get() / 1000000000.0, models.size(),
					config.REMARK + e.getMessage());
			throw new SQLException(e.getMessage());
		}
	}

	private LinkedList<TSDBDataModel> createDataModel(int batchIndex, int dataIndex, String device) {
		LinkedList<TSDBDataModel> models = new LinkedList<TSDBDataModel>();
		int deviceNum = getDeviceNum(device);
		int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
		int groupNum = deviceNum / groupSize;
		String groupId = "group_" + groupNum;
		String metricName = metric + groupId;
		for (String sensor : config.SENSOR_CODES) {
			FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
			long currentTime = Constants.START_TIMESTAMP
					+ config.POINT_STEP * (batchIndex * config.CACHE_NUM + dataIndex);
			Number value = Function.getValueByFuntionidAndParam(param, currentTime);
			TSDBDataModel model = new TSDBDataModel();
			model.setMetric(metricName);
			model.setTimestamp(currentTime);
			model.setValue(value);
			Map<String, String> tags = new HashMap<>();
			tags.put("device", device);
			tags.put("sensor", sensor);
			model.setTags(tags);
			models.addLast(model);
		}
		return models;
	}

	private LinkedList<TSDBDataModel> createDataModel(int timestampIndex, String device) {
		LinkedList<TSDBDataModel> models = new LinkedList<TSDBDataModel>();
		int deviceNum = getDeviceNum(device);
		int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
		int groupNum = deviceNum / groupSize;
		String groupId = "group_" + groupNum;
		String metricName = metric + groupId;
		for (String sensor : config.SENSOR_CODES) {
			FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
			long currentTime;
			if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
				currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * timestampIndex + (long) (config.POINT_STEP * timestampRandom.nextDouble());
			} else {
				currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * timestampIndex;
			}
			Number value = Function.getValueByFuntionidAndParam(param, currentTime);
			TSDBDataModel model = new TSDBDataModel();
			model.setMetric(metricName);
			model.setTimestamp(currentTime);
			model.setValue(value);
			Map<String, String> tags = new HashMap<>();
			tags.put("device", device);
			tags.put("sensor", sensor);
			model.setTags(tags);
			models.addLast(model);
		}
		return models;
	}

	private int getDeviceNum(String device) {
		String[] parts = device.split("_");
		try {
			return Integer.parseInt(parts[1]);
		} catch (Exception e) {
			LOGGER.error("{} {}", device, e.getMessage());
			throw e;
		}
	}

	@Override
	public long getTotalTimeInterval() throws SQLException {
		long endTime = 250000000 + Constants.START_TIMESTAMP;
		Map<String, Object> queryMap = new HashMap<>();

		Map<String, Object> subQuery = new HashMap<String, Object>();
		subQuery.put("metric", getMetricName(0));
		Map<String, String> tags = new HashMap<String, String>();
		tags.put("device", "d_0");
		tags.put("sensor", "s_0");
		subQuery.put("tags", tags);
		List<Map<String, Object>> list = new ArrayList<>();
		list.add(subQuery);

		queryMap.put("queries", list);
		queryMap.put("backScan", backScanTime);

		LOGGER.debug(JSON.toJSONString(queryMap));
		try {
			String str = HttpRequest.sendPost(queryUrl + "/last", JSON.toJSONString(queryMap));
			LOGGER.debug(str);
			JSONArray jsonArray = new JSONArray(str);
			if (jsonArray.length() > 0) {
				endTime = ((JSONObject) jsonArray.get(0)).getLong("timestamp");
				LOGGER.debug("endTime = {}", endTime);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return endTime - Constants.START_TIMESTAMP;
	}

	@Override
	public void executeOneQuery(List<Integer> devices, int index, long startTime, QueryClientThread client,
			ThreadLocal<Long> errorCount) {
		String sql = "";
		long startTimeStamp = 0, endTimeStamp = 0;
		Map<String, Object> queryMap = new HashMap<>();
		List<Map<String, Object>> list = null;
		queryMap.put("msResolution", true);
		queryMap.put("start", startTime);
		queryMap.put("end", startTime + config.QUERY_INTERVAL);

		try {
			List<String> sensorList = new ArrayList<String>();
			switch (config.QUERY_CHOICE) {
			case 1:// 精确点查询
				long timeStamp = (startTime - Constants.START_TIMESTAMP) / config.POINT_STEP * config.POINT_STEP
						+ Constants.START_TIMESTAMP;
				if (config.IS_EMPTY_PRECISE_POINT_QUERY) {
					timeStamp += config.POINT_STEP / 2;
				}
				queryMap.put("start", timeStamp - 1);
				queryMap.put("end", timeStamp + 1);
				list = getSubQueries(devices);
				queryMap.put("queries", list);
				break;
			case 2:// 模糊点查询（暂未实现）
				break;
			case 3:// 聚合函数查询
				list = getSubQueries(devices);
				for (Map<String, Object> subQuery : list) {
					subQuery.put("downsample", config.QUERY_INTERVAL + "ms-" + config.QUERY_AGGREGATE_FUN);
				}
				queryMap.put("queries", list);
				break;
			case 4:// 范围查询
				list = getSubQueries(devices);
				queryMap.put("queries", list);
				break;
			case 5:// 条件查询

				break;
			case 6:// 最近点查询
//				queryMap.clear();
//				list = getSubQueries(devices);
//				for (Map<String, Object> subQuery : list) {
//					subQuery.remove("aggregator");
//				}
//				queryMap.put("queries", list);
//				queryMap.put("backScan", backScanTime);


				list = getSubQueries(devices);
				for (Map<String, Object> subQuery : list) {
					subQuery.remove("end");
				}
				for (Map<String, Object> subQuery : list) {
					subQuery.remove("aggregator");
				}
				for (Map<String, Object> subQuery : list) {
					subQuery.put("aggregator", "none");
				}
				for (Map<String, Object> subQuery : list) {
					subQuery.put("downsample", "0all-last");
				}
				queryMap.put("queries", list);
				break;
			case 7:// groupBy查询（暂时只有一个时间段）
				list = getSubQueries(devices);
				for (Map<String, Object> subQuery : list) {
					subQuery.put("downsample", config.TIME_UNIT + "ms-" + config.QUERY_AGGREGATE_FUN);
				}
				queryMap.put("queries", list);
				break;
			}
			sql = JSON.toJSONString(queryMap);
			LOGGER.debug("JSON.toJSONString(queryMap): "+sql);
			
			String str = null;
			if(config.QUERY_CHOICE != 6) {
				startTimeStamp = System.nanoTime();
				str = HttpRequest.sendPost(queryUrl, sql);
				endTimeStamp = System.nanoTime();
			}
			else {
				startTimeStamp = System.nanoTime();
//				str = HttpRequest.sendPost(queryUrl+"/last", sql);
				str = HttpRequest.sendPost(queryUrl, sql);
				endTimeStamp = System.nanoTime();
			}
			LOGGER.debug("Response: "+str);
			
			int pointNum = getOneQueryPointNum(str);
			client.setTotalPoint(client.getTotalPoint() + pointNum);
			client.setTotalTime(client.getTotalTime() + endTimeStamp - startTimeStamp);
			LOGGER.info(
					"{} execute {} loop, it costs {}s with {} result points cur_rate is {}points/s; "
							+ "TotalTime {}s with totalPoint {} rate is {}points/s",
					Thread.currentThread().getName(), index, (endTimeStamp - startTimeStamp) / 1000000000.0, pointNum,
					pointNum * 1000000000.0 / (endTimeStamp - startTimeStamp), (client.getTotalTime()) / 1000000000.0,
					client.getTotalPoint(), client.getTotalPoint() * 1000000000.0f / client.getTotalTime());
			mySql.saveQueryProcess(index, pointNum, (endTimeStamp - startTimeStamp) / 1000000000.0f, config.REMARK);
		} catch (Exception e) {
			queryErrorProcess(index, errorCount, sql, startTimeStamp, endTimeStamp, e, LOGGER, mySql);
		}
	}

	private int getOneQueryPointNum(String str) {
		int pointNum = 0;
		if(config.QUERY_CHOICE != 6) {
			JSONArray jsonArray = new JSONArray(str);
			for (int i = 0; i < jsonArray.length(); i++) {
				JSONObject json = (JSONObject) jsonArray.get(i);
				pointNum += json.getJSONObject("dps").length();
			}
		}
		else {
			JSONArray jsonArray = new JSONArray(str);
			pointNum += jsonArray.length();
		}
		return pointNum;
	}

	private String getMetricName(Integer deviceNum) {
		int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
		int groupNum = deviceNum / groupSize;
		String groupId = "group_" + groupNum;
		return metric + groupId;
	}

	private List<Map<String, Object>> getSubQueries(List<Integer> devices) {
		List<Map<String, Object>> list = new ArrayList<>();

		List<String> sensorList = new ArrayList<String>();
		for (String sensor : config.SENSOR_CODES) {
			sensorList.add(sensor);
		}
		Collections.shuffle(sensorList, sensorRandom);

		Map<String, List<Integer>> metric2devices = new HashMap<String, List<Integer>>();
		for (int d : devices) {
			String m = getMetricName(d);
			metric2devices.putIfAbsent(m, new ArrayList<Integer>());
			metric2devices.get(m).add(d);
		}

		for (Entry<String, List<Integer>> queryMetric : metric2devices.entrySet()) {
			Map<String, Object> subQuery = new HashMap<String, Object>();
			subQuery.put("aggregator", config.QUERY_AGGREGATE_FUN);// FIXME 值的意义需要再研究一下
			subQuery.put("metric", queryMetric.getKey());

			Map<String, String> tags = new HashMap<String, String>();
			String deviceStr = "";
			for (int d : queryMetric.getValue()) {
				deviceStr+="|"+config.DEVICE_CODES.get(d);
			}
			deviceStr=deviceStr.substring(1);
			
			String sensorStr = sensorList.get(0);
			for (int i = 1; i < config.QUERY_SENSOR_NUM; i++) {
				sensorStr += "|"+sensorList.get(i);
			}
			tags.put("sensor", sensorStr);
			tags.put("device", deviceStr);
			subQuery.put("tags", tags);
			list.add(subQuery);
		}
		return list;
	}

	@Override
	public void createSchema() throws SQLException {
		// no need for opentsdb

	}

	@Override
	public long getLabID() {
		return this.labID;
	}

	@Override
	public void exeSQLFromFileByOneBatch() throws SQLException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public int insertOverflowOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Integer> before, Integer maxTimestampIndex, Random random) throws SQLException {
		return 0;
	}

	@Override
	public int insertOverflowOneBatchDist(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, Integer maxTimestampIndex, Random random) throws SQLException {
		int timestampIndex;
		PossionDistribution possionDistribution = new PossionDistribution(random);
		int nextDelta;
		LinkedList<String> keys = new LinkedList<>();
		for (int i = 0; i < config.CACHE_NUM; i++) {
			if (probTool.returnTrueByProb(config.OVERFLOW_RATIO, random)) {
				nextDelta = possionDistribution.getNextPossionDelta();
				timestampIndex = maxTimestampIndex - nextDelta;
			} else {
				maxTimestampIndex++;
				timestampIndex = maxTimestampIndex;
			}
			String key = UUID.randomUUID().toString();
			dataMap.put(key, createDataModel(timestampIndex, device));
			keys.add(key);
		}
		insertOneBatch(keys, loopIndex, totalTime, errorCount);
		return maxTimestampIndex;
	}

	@Override
	public void insertOneBatchMulDevice(LinkedList<String> deviceCodes, int batchIndex, ThreadLocal<Long> totalTime,
			ThreadLocal<Long> errorCount) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public long count(String group, String device, String sensor) {
		// TODO Auto-generated method stub

		return 0;
	}

	@Override
	public void createSchemaOfDataGen() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void insertGenDataOneBatch(String device, int i, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws SQLException {
		// TODO Auto-generated method stub

	}

}
