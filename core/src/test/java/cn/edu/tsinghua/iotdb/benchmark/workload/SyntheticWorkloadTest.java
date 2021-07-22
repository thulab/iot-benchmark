package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * SyntheticWorkload Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>Mar 18, 2019</pre>
 */
public class SyntheticWorkloadTest {

  private static Config config = ConfigDescriptor.getInstance().getConfig();

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  /**
   * Method: getOneBatch()
   */
  @Test
  public void testGetOneBatch() throws Exception {
//TODO: Test goes here... 
  }

  /**
   * Method: getPreciseQuery()
   */
  @Test
  public void testGetPreciseQuery() throws Exception {
//TODO: Test goes here... 
  }

  /**
   * Method: getRangeQuery()
   */
  @Test
  public void testGetRangeQuery() throws Exception {
//TODO: Test goes here... 
  }

  /**
   * Method: getValueRangeQuery()
   */
  @Test
  public void testGetValueRangeQuery() throws Exception {
//TODO: Test goes here... 
  }

  /**
   * Method: getAggRangeQuery()
   */
  @Test
  public void testGetAggRangeQuery() throws Exception {
//TODO: Test goes here... 
  }

  /**
   * Method: getAggValueQuery()
   */
  @Test
  public void testGetAggValueQuery() throws Exception {
//TODO: Test goes here... 
  }

  /**
   * Method: getAggRangeValueQuery()
   */
  @Test
  public void testGetAggRangeValueQuery() throws Exception {
//TODO: Test goes here... 
  }

  /**
   * Method: getGroupByQuery()
   */
  @Test
  public void testGetGroupByQuery() throws Exception {
//TODO: Test goes here... 
  }

  /**
   * Method: getLatestPointQuery()
   */
  @Test
  public void testGetLatestPointQuery() throws Exception {
//TODO: Test goes here... 
  }


  /**
   * Method: getOrderedBatch()
   */
  @Test
  public void testGetOrderedBatch() throws Exception {
    config.setBATCH_SIZE_PER_WRITE(5);
    config.setPOINT_STEP(5000L);
    config.setIS_REGULAR_FREQUENCY(false);
    SyntheticWorkload syntheticWorkload = new SyntheticWorkload(1);
    for (int i = 0; i < 3; i++) {
      Batch batch = syntheticWorkload.getOneBatch(new DeviceSchema(1), i);
      long old = 0;
      for(Record record: batch.getRecords()){
        // 检查map里timestamp获取到的是否是按序的
        assertTrue(record.getTimestamp() > old);
        old = record.getTimestamp();
      }
      System.out.println(batch.getRecords().toString());
    }

  }

  /**
   * Method: getDistOutOfOrderBatch()
   */
  @Test
  public void testGetDistOutOfOrderBatch() throws Exception {
//TODO: Test goes here... 
/*
try {
   Method method = SyntheticWorkload.getClass().getMethod("getDistOutOfOrderBatch");
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) {
} 
*/
  }

  /**
   * Method: getLocalOutOfOrderBatch()
   */
  @Test
  public void testGetLocalOutOfOrderBatch() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = SyntheticWorkload.getClass().getMethod("getLocalOutOfOrderBatch");
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
  }

  /**
   * Method: getGlobalOutOfOrderBatch()
   */
  @Test
  public void testGetGlobalOutOfOrderBatch() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = SyntheticWorkload.getClass().getMethod("getGlobalOutOfOrderBatch");
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
  }

} 
