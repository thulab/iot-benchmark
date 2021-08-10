package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import java.util.List;

/**
 * @Author stormbroken
 * Create by 2021/08/10
 * @Version 1.0
 **/

public class RealDataWorkload implements IRealDataWorkload{
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    private List<DeviceSchema> deviceSchemaList;
    private BasicReader basicReader;

    /**
     * Init reader of real dataset write test
     *
     * @param files real dataset files
     */
    public RealDataWorkload(List<String> files) {
        // file -> device
        // a file -> a batch
        // Precise should do the same thing to read files
        switch (config.getDATA_SET()) {
            case TDRIVE:
                basicReader = new TDriveReader(config, files);
                break;
            case REDD:
                basicReader = new ReddReader(config, files);
                break;
            case GEOLIFE:
                basicReader = new GeolifeReader(config, files);
                break;
            case NOAA:
                basicReader = new NOAAReader(config, files);
                break;
            default:
                throw new RuntimeException(config.getDATA_SET() + " not supported");
        }
    }

    /**
     * Return a batch from real data
     * return null if there is no data
     *
     * @return
     * @throws WorkloadException
     */
    @Override
    public Batch getOneBatch() throws WorkloadException {
        if (basicReader.hasNextBatch()) {
            return basicReader.nextBatch();
        } else {
            return null;
        }
    }

    /**
     * Get precise query Eg. select v1... from data where time = ? and device in ?
     *
     * @return
     * @throws WorkloadException
     */
    @Override
    public PreciseQuery getPreciseQuery() throws WorkloadException {
        return null;
    }

    /**
     * Get range query Eg. select v1... from data where time > ? and time < ? and device in ?
     *
     * @return
     * @throws WorkloadException
     */
    @Override
    public RangeQuery getRangeQuery() throws WorkloadException {
        return null;
    }

    /**
     * Get value range query Eg. select v1... from data where time > ? and time < ? and v1 > ? and
     * device in ?
     *
     * @return
     * @throws WorkloadException
     */
    @Override
    public ValueRangeQuery getValueRangeQuery() throws WorkloadException {
        return null;
    }

    /**
     * Get aggregate range query Eg. select func(v1)... from data where device in ? and time > ? and
     * time < ?
     *
     * @return
     * @throws WorkloadException
     */
    @Override
    public AggRangeQuery getAggRangeQuery() throws WorkloadException {
        return null;
    }

    /**
     * Get aggregate value query Eg. select func(v1)... from data where device in ? and value > ?
     *
     * @return
     * @throws WorkloadException
     */
    @Override
    public AggValueQuery getAggValueQuery() throws WorkloadException {
        return null;
    }

    /**
     * Get aggregate range value query Eg. select func(v1)... from data where device in ? and value >
     * ? and time > ? and time < ?
     *
     * @return
     * @throws WorkloadException
     */
    @Override
    public AggRangeValueQuery getAggRangeValueQuery() throws WorkloadException {
        return null;
    }

    /**
     * Get group by query Now only sentences with one time interval can be generated
     *
     * @return
     * @throws WorkloadException
     */
    @Override
    public GroupByQuery getGroupByQuery() throws WorkloadException {
        return null;
    }

    /**
     * Get latest point query Eg. select time, v1... where device = ? and time = max(time)
     *
     * @return
     * @throws WorkloadException
     */
    @Override
    public LatestPointQuery getLatestPointQuery() throws WorkloadException {
        return null;
    }

    /**
     * Return device schemas generate from real data
     *
     * @return
     */
    @Override
    public List<DeviceSchema> getDeviceSchema() {
        return deviceSchemaList;
    }
}
