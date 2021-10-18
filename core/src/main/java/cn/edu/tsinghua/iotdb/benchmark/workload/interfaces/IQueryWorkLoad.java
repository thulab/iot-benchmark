package cn.edu.tsinghua.iotdb.benchmark.workload.interfaces;

import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;

public interface IQueryWorkLoad extends IWorkLoad {
  /**
   * Get precise query Eg. select v1... from data where time = ? and device in ?
   *
   * @return
   * @throws WorkloadException
   */
  PreciseQuery getPreciseQuery() throws WorkloadException;

  /**
   * Get range query Eg. select v1... from data where time > ? and time < ? and device in ?
   *
   * @return
   * @throws WorkloadException
   */
  RangeQuery getRangeQuery() throws WorkloadException;

  /**
   * Get value range query Eg. select v1... from data where time > ? and time < ? and v1 > ? and
   * device in ?
   *
   * @return
   * @throws WorkloadException
   */
  ValueRangeQuery getValueRangeQuery() throws WorkloadException;

  /**
   * Get aggregate range query Eg. select func(v1)... from data where device in ? and time > ? and
   * time < ?
   *
   * @return
   * @throws WorkloadException
   */
  AggRangeQuery getAggRangeQuery() throws WorkloadException;

  /**
   * Get aggregate value query Eg. select func(v1)... from data where device in ? and value > ?
   *
   * @return
   * @throws WorkloadException
   */
  AggValueQuery getAggValueQuery() throws WorkloadException;

  /**
   * Get aggregate range value query Eg. select func(v1)... from data where device in ? and value >
   * ? and time > ? and time < ?
   *
   * @return
   * @throws WorkloadException
   */
  AggRangeValueQuery getAggRangeValueQuery() throws WorkloadException;

  /**
   * Get group by query Now only sentences with one time interval can be generated
   *
   * @return
   * @throws WorkloadException
   */
  GroupByQuery getGroupByQuery() throws WorkloadException;

  /**
   * Get latest point query Eg. select time, v1... where device = ? and time = max(time)
   *
   * @return
   * @throws WorkloadException
   */
  LatestPointQuery getLatestPointQuery() throws WorkloadException;

  /**
   * Return a verified Query
   *
   * @param batch have the data to check
   * @return
   * @throws WorkloadException
   */
  VerificationQuery getVerifiedQuery(Batch batch) throws WorkloadException;

  /**
   * Get device query, Eg. select time, v1... where device = ?
   *
   * @return
   * @throws WorkloadException
   */
  DeviceQuery getDeviceQuery();
}
