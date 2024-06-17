package cn.edu.tsinghua.iot.benchmark.iotdb130;

import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.entity.Record;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iot.benchmark.measurement.Status;
import cn.edu.tsinghua.iot.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iot.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.ValueRangeQuery;
import com.google.gson.Gson;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RestAPI extends IoTDB {
    private final OkHttpClient client = new OkHttpClient();
    private DBConfig dbConfig;
    private final String baseURL;
    private final String authorization = "Basic cm9vdDpyb290";
    protected final String ROOT_SERIES_NAME;
    protected static final Config config = ConfigDescriptor.getInstance().getConfig();

    public RestAPI(DBConfig dbConfig) {
        super(dbConfig);
        this.dbConfig = dbConfig;
        String host = dbConfig.getHOST().get(0);
        baseURL = String.format("http://%s:18080", host);
        ROOT_SERIES_NAME = "root";
    }

    private Request constructRequest(String api, String json) {
        RequestBody jsonBody = RequestBody.create(MediaType.parse("application/json"), json);
        return new Request.Builder()
                .url(String.format("%s%s", baseURL, api))
                .header("Authorization", authorization)
                .post(jsonBody)
                .build();
    }

    @Override
    public void init() throws TsdbException {}

    @Override
    public void cleanup() {
        String json = "{\"sql\":\"delete database root.**\"}";
        Request request = constructRequest("/rest/v2/nonQuery", json);
        try {
            Response response = client.newCall(request).execute();
            response.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @Override
    public void close() throws TsdbException {}

    @Override
    public Status insertOneBatch(IBatch batch) throws DBConnectException {
        String json = generatePayload(batch);
        Request request = constructRequest("/rest/v2/insertTablet", json);
        try {
            Response response = client.newCall(request).execute();
            response.close();
            return new Status(true);
        } catch (IOException e) {
            System.out.println(e.getMessage());
            return new Status(false);
        }
    }

    private String generatePayload(IBatch batch) {
        DeviceSchema schema = batch.getDeviceSchema();
        Payload payload = new Payload();
        payload.device = String.format("root.%s", schema.getDevicePath());
        payload.is_aligned = config.isIS_SENSOR_TS_ALIGNMENT();

        List<String> measurements = new ArrayList<>();
        List<String> dataTypes = new ArrayList<>();
        for (Sensor sensor : schema.getSensors()) {
            measurements.add(sensor.getName());
            dataTypes.add(sensor.getSensorType().name);
        }
        payload.measurements = measurements;
        payload.data_types = dataTypes;

        List<Long> timestamps = new ArrayList<>();
        List<List<Object>> values = new ArrayList<>();
        for (Record record : batch.getRecords()) {
            timestamps.add(record.getTimestamp());
            List<Object> row = record.getRecordDataValue();
            for (int j = 0; j < row.size(); j++) {
                if (values.size() <= j) {
                    values.add(new ArrayList<>());
                }
                values.get(j).add(row.get(j));
            }
        }
        payload.timestamps = timestamps;
        payload.values = values;

        return new Gson().toJson(payload);
    }

    @Override
    protected Status executeQueryAndGetStatus(String sql, Operation operation){
        return executeQueryAndGetStatus(sql);
    }

    private Status executeQueryAndGetStatus(String sql) {
        String json = String.format("{\"sql\":\"%s\"}", sql);
        Request request = constructRequest("/rest/v2/query", json);
        try {
            Response response = client.newCall(request).execute();
            String body = response.body().string();
            QueryResult queryResult = new Gson().fromJson(body, QueryResult.class);
            response.close();
            if (queryResult.timestamps == null && response.code() == 200) {
                // The aggregate query has no timestamps and only one result
                return new Status(true);
            }else{
                return new Status(true, queryResult.timestamps.size());
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
            return new Status(false);
        }
    }


    private class Payload {
        public String device;
        public boolean is_aligned;
        public List<List<Object>> values;
        public List<String> data_types;
        public List<String> measurements;
        public List<Long> timestamps;
    }

    private class QueryResult {
        public List<String> expressions;
        public List<String> column_names;
        public List<Long> timestamps;
        public List<List<Object>> values;
    }
}
