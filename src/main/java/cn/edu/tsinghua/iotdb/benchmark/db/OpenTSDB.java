package cn.edu.tsinghua.iotdb.benchmark.db;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
 * @author fasape
 *
 */
public class OpenTSDB implements IDatebase {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenTSDB.class);
	private String openUrl;
	private String queryUrl;
	private String writeUrl;
	private String metric="root.perform";
	private Config config;
	private MySqlLog mySql=new MySqlLog();;
	private long labID;
	private Map<String,LinkedList<TSDBDataModel>> dataMap=new HashMap<>();
	private static final OkHttpClient OK_HTTP_CLIENT = new OkHttpClient().newBuilder()
		       .readTimeout(500000, TimeUnit.MILLISECONDS)
		       .connectTimeout(500000, TimeUnit.MILLISECONDS)
		       .writeTimeout(500000, TimeUnit.MILLISECONDS)
		       .build();
	private  static OkHttpClient getOkHttpClient(){
		return OK_HTTP_CLIENT;
	}
	public OpenTSDB(long labID) {
		mySql = new MySqlLog();
		this.labID = labID;
	}
	@Override
	public void init() throws SQLException {
		config=ConfigDescriptor.getInstance().getConfig();
		//FIXME 
		openUrl=config.OPENTSDB_URL;
		writeUrl=openUrl+"/api/put";
		queryUrl=openUrl+"/api/query";
		mySql.initMysql(labID);
	}

	@Override
	public void createSchema() throws SQLException {
		// no need for opentsdb

	}

	@Override
	public void insertOneBatch(String device, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount)
			throws SQLException {
        LinkedList<String> keys = new LinkedList<>();
        for (int i = 0; i < config.CACHE_NUM; i++) {
        		String key=UUID.randomUUID().toString();
        		dataMap.put(key, createDataModel(batchIndex, i, device));
        }
        insertOneBatch(keys, batchIndex, totalTime, errorCount);
	}

	private LinkedList<TSDBDataModel> createDataModel(int batchIndex, int dataIndex, String device){
		LinkedList<TSDBDataModel> models=new LinkedList<TSDBDataModel>();
        int deviceNum = getDeviceNum(device);
        int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
        int groupNum = deviceNum / groupSize;
        String groupId = "group_"+groupNum;
//        String metric="root."+groupId;
        for(String sensor: config.SENSOR_CODES){
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            long currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * (batchIndex * config.CACHE_NUM + dataIndex);
            Number value = Function.getValueByFuntionidAndParam(param, currentTime);
            TSDBDataModel model = new TSDBDataModel();
            model.setMetric(metric);
            model.setTimestamp(currentTime);
            model.setValue(value);
            Map<String,String> tags=new HashMap<>();
            tags.put("g", groupId);
            tags.put("d", device);
            tags.put("s", sensor);
            models.addLast(model);
        }
        return  models;
	}
    private int getDeviceNum(String device) {
        String[] parts = device.split("_");
        try {
            return Integer.parseInt(parts[1]);
        } catch (Exception e) {
            LOGGER.error("{} {}",device, e.getMessage());
            throw e;
        }
    }

	@Override
	public void close() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void flush() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void getUnitPointStorageSize() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public long getTotalTimeInterval() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void executeOneQuery(List<Integer> devices, int index, long startTime, QueryClientThread client,
			ThreadLocal<Long> errorCount) {
		String sql = "";
		long startTimeStamp = 0, endTimeStamp = 0;
		Map<String,Object> queryMap=new HashMap<>();
		try {
			List<String> sensorList = new ArrayList<String>();
			switch (config.QUERY_CHOICE) {
			case 1:// 精确点查询
				long timeStamp = (startTime - Constants.START_TIMESTAMP) / config.POINT_STEP * config.POINT_STEP
						+ Constants.START_TIMESTAMP;
				if (config.IS_EMPTY_PRECISE_POINT_QUERY) {
					timeStamp += config.POINT_STEP / 2;
				}
				
				break;
			case 2:// 模糊点查询（暂未实现）
				break;
			case 3:// 聚合函数查询
				break;
			case 4:// 范围查询
				break;
			case 5:// 条件查询
				
				break;
			case 6:// 最近点查询
				break;
			case 7:// groupBy查询（暂时只有一个时间段）
				queryMap.put("start",startTime);
				queryMap.put("end", startTime+config.QUERY_INTERVAL);
				Map<String,Object> subQuery = new HashMap<String ,Object>();
				subQuery.put("aggregator", "avg");////FIXME 值的意义需要再研究一下
				subQuery.put("metric", metric);
				subQuery.put("downsample", "1m-avg");//FIXME 根据method进行计算
//				Map<String,Object> subTag = new HashMap<String ,Object>();
//				subTag.put("d",point.getDeviceCode());
//				subTag.put("s",point.getSensorCode());
//				subQuery.put("tags", subTag);	
				List<Map<String,Object>> list = new ArrayList<>(); 
				list.add(subQuery);
				queryMap.put("queries",list);
				break;
			}
			int line = 0;
			startTimeStamp = System.currentTimeMillis();
			//TODO 执行查询
			//LOGGER.info("{}", builder.toString());
			String sendPost = HttpRequest.sendPost(queryUrl, JSON.toJSONString(queryMap));
			endTimeStamp = System.currentTimeMillis();
			
			client.setTotalPoint(client.getTotalPoint() + line * config.QUERY_SENSOR_NUM);
			client.setTotalTime(client.getTotalTime() + endTimeStamp - startTimeStamp);
			LOGGER.info(
					"{} execute {} loop, it costs {}s with {} result points cur_rate is {}points/s; "
							+ "TotalTime {}s with totalPoint {} rate is {}points/s",
					Thread.currentThread().getName(), index, (endTimeStamp - startTimeStamp) / 1000.0,
					line * config.QUERY_SENSOR_NUM,
					line * config.QUERY_SENSOR_NUM * 1000.0 / (endTimeStamp - startTimeStamp),
					(client.getTotalTime()) / 1000.0, client.getTotalPoint(),
					client.getTotalPoint() * 1000.0f / client.getTotalTime());
			mySql.saveQueryProcess(index, line * config.QUERY_SENSOR_NUM, (endTimeStamp - startTimeStamp) / 1000.0f,
					config.REMARK);
		} catch (Exception e) {
			errorCount.set(errorCount.get() + 1);
			LOGGER.error("{} execute query failed! Error：{}", Thread.currentThread().getName(), e.getMessage());
			LOGGER.error("执行失败的查询语句：{}", sql);
			mySql.saveQueryProcess(index, 0, (endTimeStamp - startTimeStamp) / 1000.0f, "query fail!" + sql);
			e.printStackTrace();
		}
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
	public void exeSQLFromFileByOneBatch() throws SQLException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void insertOneBatch(LinkedList<String> keys, int batchIndex, ThreadLocal<Long> totalTime,
			ThreadLocal<Long> errorCount) throws SQLException {
		long startTime = 0, endTime = 0;
		LinkedList<TSDBDataModel> models=new LinkedList<>();
		for(String key:keys) {
			models.addAll(dataMap.get(key));
			dataMap.remove(key);
		}
		String body=JSON.toJSONString(models);
        try {
            startTime = System.currentTimeMillis();
            String response = HttpRequest.sendPost(writeUrl, body);
            endTime = System.currentTimeMillis();
            LOGGER.info("{} execute {} batch, it costs {}s, totalTime{}, throughput {} items/s",
                    Thread.currentThread().getName(),
                    batchIndex,
                    (endTime-startTime)/1000.0,
                    ((totalTime.get()+(endTime-startTime))/1000.0),
                    (models.size() / (double) (endTime-startTime))*1000);
            totalTime.set(totalTime.get()+(endTime-startTime));
            LOGGER.info(response);
            mySql.saveInsertProcess(batchIndex, (endTime - startTime) / 1000.0, totalTime.get() / 1000.0, 0,
					config.REMARK);
        } catch (IOException e) {
            errorCount.set(errorCount.get() + models.size());
            LOGGER.error("Batch insert failed, the failed num is {}! Error：{}",
            		models.size(), e.getMessage());
			mySql.saveInsertProcess(batchIndex, (endTime - startTime) / 1000.0, totalTime.get() / 1000.0, models.size(),
					config.REMARK + e.getMessage());
           throw  new SQLException(e.getMessage());
        }
	}
	private String exeOkHttpRequest(Request request) throws Exception{
	    Response response;
	    OkHttpClient client = getOkHttpClient();
		response = client.newCall(request).execute();
		int code = response.code();
		response.close();
		return code+"";
	}
}
