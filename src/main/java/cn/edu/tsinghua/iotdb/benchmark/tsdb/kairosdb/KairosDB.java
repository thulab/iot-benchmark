package cn.edu.tsinghua.iotdb.benchmark.tsdb.kairosdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.model.KairosDataModel;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.utils.HttpRequest;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import com.alibaba.fastjson.JSON;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.response.Queries;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Results;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KairosDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(cn.edu.tsinghua.iotdb.benchmark.db.kairosdb.KairosDB.class);
  private String Url;
  private String queryUrl;
  private String writeUrl;
  private String deleteUrl;
  private String dataType = "double";
  private Config config;

  private static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
  private SimpleDateFormat sdf = new SimpleDateFormat(TIME_FORMAT);

  public KairosDB() {
    config = ConfigDescriptor.getInstance().getConfig();
    Url = config.DB_URL;
    queryUrl = Url + "/api/v1/datapoints/query";
    writeUrl = Url + "/api/v1/datapoints";
    deleteUrl = Url + "/api/v1/metric/%s";
  }

  @Override
  public void init() throws TsdbException {
    //no need for KairosDB
  }

  @Override
  public void cleanup() throws TsdbException {
    try {
      HttpClient client = new HttpClient(config.DB_URL);
      for (String sensor : config.SENSOR_CODES) {
        client.deleteMetric(sensor);
      }
    } catch (Exception e) {
      LOGGER.error("Delete old data failed because ", e);
    }
  }

  @Override
  public void close() throws TsdbException {
    //no need for KairosDB
  }

  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    //no need for KairosDB
  }


  private LinkedList<KairosDataModel> createDataModel(DeviceSchema deviceSchema, long timestamp,
      List<String> recordValues) {
    LinkedList<KairosDataModel> models = new LinkedList<>();
    String groupId = deviceSchema.getGroup();
    int i = 0;
    for (String sensor : deviceSchema.getSensors()) {
      KairosDataModel model = new KairosDataModel();
      model.setName(sensor);
      // KairosDB do not support float as data type
      if (!config.DATA_TYPE.equals("FLOAT")) {
        model.setType("double");
      } else {
        model.setType(config.DATA_TYPE.toLowerCase());
      }
      model.setTimestamp(timestamp);
      model.setValue(recordValues.get(i));
      Map<String, String> tags = new HashMap<>();
      tags.put("group", groupId);
      tags.put("device", deviceSchema.getDevice());
      model.setTags(tags);
      models.addLast(model);
      i++;
    }
    return models;
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    long st;
    long en;
    LinkedList<KairosDataModel> models = new LinkedList<>();
    for (Record record : batch.getRecords()) {
      models.addAll(createDataModel(batch.getDeviceSchema(), record.getTimestamp(),
          record.getRecordDataValue()));
    }
    String body = JSON.toJSONString(models);
    LOGGER.debug("body: {}", body);
    try {
      st = System.nanoTime();
      String response = HttpRequest.sendPost(writeUrl, body);
      en = System.nanoTime();
      LOGGER.debug("response: {}", response);
      return new Status(true, en - st);
    } catch (Exception e) {
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    return null;
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    QueryBuilder builder = QueryBuilder.getInstance();
    long st;
    long en;
    int queryResultPointNum = 0;
    try {
      HttpClient client = new HttpClient(config.DB_URL);
      builder.setStart(new Date(rangeQuery.getStartTimestamp()))
          .setEnd(new Date(rangeQuery.getEndTimestamp()));
      for (DeviceSchema deviceSchema : rangeQuery.getDeviceSchema()) {
        for (String sensor : deviceSchema.getSensors()) {
          builder.addMetric(sensor)
              .addTag("device", deviceSchema.getDevice())
              .addTag("group", deviceSchema.getGroup());
        }
      }
      st = System.nanoTime();
      QueryResponse response = client.query(builder);
      en = System.nanoTime();
      for (Queries query : response.getQueries()) {
        for (Results results : query.getResults()) {
          queryResultPointNum += results.getDataPoints().size();
        }
      }
      return new Status(true, en - st, queryResultPointNum);
    } catch (Exception e) {
      return new Status(false, 0, queryResultPointNum, e, builder.toString());
    }
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    return null;
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    return null;
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    return null;
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    return null;
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    return null;
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    return null;
  }
}
