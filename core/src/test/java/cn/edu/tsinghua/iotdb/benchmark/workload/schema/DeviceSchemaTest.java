package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** 
* DeviceSchema Tester. 
* 
* @author <Authors name> 
* @since <pre>Mar 18, 2019</pre> 
* @version 1.0 
*/ 
public class DeviceSchemaTest {

  private String strategy = null;

  @Before
  public void setUp() {
    strategy = ConfigDescriptor.getInstance().getConfig().getSG_STRATEGY();
    ConfigDescriptor.getInstance().getConfig().setSG_STRATEGY(Constants.MOD_SG_ASSIGN_MODE);
  }

  @After
  public void tearDown() {
    ConfigDescriptor.getInstance().getConfig().setSG_STRATEGY(strategy);
  }

  /**
  *
  * Method: CalGroupId()
  *
  */
  @Test
  public void testCalGroupId() throws Exception {
    int ori = ConfigDescriptor.getInstance().getConfig().getGROUP_NUMBER();
    DeviceSchema schema = new DeviceSchema(0);
    ConfigDescriptor.getInstance().getConfig().setGROUP_NUMBER(3);
    int g1 = schema.calGroupId(4);
    assertEquals(1,g1 );

    ConfigDescriptor.getInstance().getConfig().setGROUP_NUMBER(3);
    int g2 = schema.calGroupId(3);
    assertEquals(0, g2);

    ConfigDescriptor.getInstance().getConfig().setGROUP_NUMBER(30);
    int g3 = schema.calGroupId(30);
    assertEquals(0, g3);
    ConfigDescriptor.getInstance().getConfig().setGROUP_NUMBER(30);
    int g4 = schema.calGroupId(40);
    assertEquals(10, g4);

    ConfigDescriptor.getInstance().getConfig().setGROUP_NUMBER(3);
    int g5 = schema.calGroupId(0);
    assertEquals(0, g5);

    ConfigDescriptor.getInstance().getConfig().setGROUP_NUMBER(ori);
  }

} 
