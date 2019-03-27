package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After; 

/** 
* DeviceSchema Tester. 
* 
* @author <Authors name> 
* @since <pre>Mar 18, 2019</pre> 
* @version 1.0 
*/ 
public class DeviceSchemaTest {
  private static Config config = ConfigDescriptor.getInstance().getConfig();

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: getDeviceId() 
* 
*/ 
@Test
public void testGetDeviceId() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: setDeviceId(int deviceId) 
* 
*/ 
@Test
public void testSetDeviceId() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: getDevice() 
* 
*/ 
@Test
public void testGetDevice() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: setDevice(String device) 
* 
*/ 
@Test
public void testSetDevice() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: getGroup() 
* 
*/ 
@Test
public void testGetGroup() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: setGroup(String group) 
* 
*/ 
@Test
public void testSetGroup() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: getSensors() 
* 
*/ 
@Test
public void testGetSensors() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: setSensors(List<String> sensors) 
* 
*/ 
@Test
public void testSetSensors() throws Exception { 
//TODO: Test goes here... 
} 


/** 
* 
* Method: createEvenlyAllocDeviceSchema() 
* 
*/ 
@Test
public void testCreateEvenlyAllocDeviceSchema() throws Exception { 
  config.DEVICE_NUMBER = 10;
  config.GROUP_NUMBER = 3;
  DeviceSchema deviceSchema = new DeviceSchema(4);
  assertEquals("group_1", deviceSchema.getGroup());
  assertEquals("d_4", deviceSchema.getDevice());
} 

} 
