package cn.edu.tsinghua.iotdb.benchmark.client;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OperationControllerTest {

  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static OperationController operationController = new OperationController(0);

  @Before
  public void before() {

  }

  @After
  public void after() {

  }


  @Test
  public void testResolveOperationProportion() {
    config.setOPERATION_PROPORTION("1:1:0:1:0:1:0:1:0:0:0");
    Double[] expectedProbability = {0.2, 0.2, 0.0, 0.2, 0.0, 0.2, 0.0, 0.2, 0.0, 0.0, 0.0};
    List<Double> proportion = operationController.resolveOperationProportion();
    for (int i = 0; i < proportion.size(); i++) {
      assertEquals(expectedProbability[i], proportion.get(i));
    }
  }

  @Test
  public void testGetNextOperationType() {
    config.setOPERATION_PROPORTION("1:0:0:0:0:0:0:0:0:0:0");
    int loop = 10000;
    for(int i=0;i<loop;i++){
      assertEquals(Operation.INGESTION, operationController.getNextOperationType());
    }
    config.setOPERATION_PROPORTION("0:1:0:0:0:0:0:0:0:0:0");
    for(int i=0;i<loop;i++){
      assertEquals(Operation.PRECISE_QUERY, operationController.getNextOperationType());
    }
  }

}
