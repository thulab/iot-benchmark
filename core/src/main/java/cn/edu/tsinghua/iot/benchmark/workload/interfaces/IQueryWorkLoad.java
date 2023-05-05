package cn.edu.tsinghua.iot.benchmark.workload.interfaces;

import cn.edu.tsinghua.iot.benchmark.entity.Batch.IBatch;
import cn.edu.tsinghua.iot.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.DeviceQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.VerificationQuery;

public interface IQueryWorkLoad extends IWorkLoad {
  /** Get precise query Eg. select v1... from data where time = ? and device in ? */
  PreciseQuery getPreciseQuery() throws WorkloadException;

  /** Get range query Eg. select v1... from data where time > ? and time < ? and device in ? */
  RangeQuery getRangeQuery() throws WorkloadException;

  /**
   * Get value range query Eg. select v1... from data where time > ? and time < ? and v1 > ? and
   * device in ?
   */
  ValueRangeQuery getValueRangeQuery() throws WorkloadException;

  /**
   * Get aggregate range query Eg. select func(v1)... from data where device in ? and time > ? and
   * time < ?
   */
  AggRangeQuery getAggRangeQuery() throws WorkloadException;

  /** Get aggregate value query Eg. select func(v1)... from data where device in ? and value > ? */
  AggValueQuery getAggValueQuery() throws WorkloadException;

  /**
   * Get aggregate range value query Eg. select func(v1)... from data where device in ? and value >
   * ? and time > ? and time < ?
   */
  AggRangeValueQuery getAggRangeValueQuery() throws WorkloadException;

  /** Get group by query Now only sentences with one time interval can be generated */
  GroupByQuery getGroupByQuery() throws WorkloadException;

  /** Get latest point query Eg. select time, v1... where device = ? and time = max(time) */
  LatestPointQuery getLatestPointQuery() throws WorkloadException;

  /**
   * Return a verified Query
   *
   * @param batch have the data to check
   */
  VerificationQuery getVerifiedQuery(IBatch batch) throws WorkloadException;

  /** Get device query, Eg. select time, v1... where device = ? */
  DeviceQuery getDeviceQuery();

  /** Update query time in recent mode */
  void updateTime(long currentTimestamp);
}
