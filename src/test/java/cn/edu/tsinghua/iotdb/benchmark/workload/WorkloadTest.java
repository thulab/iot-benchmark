package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Workload Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>Mar 18, 2019</pre>
 */
public class WorkloadTest {

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
    config.BATCH_SIZE = 1;
    config.POINT_STEP = 5000;
    config.IS_RANDOM_TIMESTAMP_INTERVAL = false;
    Workload workload = new Workload(1);
    for (int i = 0; i < 3; i++) {
      Batch batch = workload.getOneBatch();
      System.out.println(batch.getDataPointsList().toString());
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
   Method method = Workload.getClass().getMethod("getDistOutOfOrderBatch"); 
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
   Method method = Workload.getClass().getMethod("getLocalOutOfOrderBatch"); 
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
   Method method = Workload.getClass().getMethod("getGlobalOutOfOrderBatch"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
  }

} 
