package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

import java.util.List;

/**
 * IRealDataWorkload is a workload using for real data
 */
public interface IRealDataWorkload extends IWorkLoad{
    /**
     * Return a batch from real data
     * return null if there is no data
     *
     * @return
     * @throws WorkloadException
     */
    Batch getOneBatch() throws WorkloadException;

    /**
     * Return device schemas generate from real data
     * @return
     */
    List<DeviceSchema> getDeviceSchema();
}
