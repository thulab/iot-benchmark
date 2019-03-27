package java.cn.edu.tsinghua.iotdb.benchmark.db; 

import org.junit.Test; 
import org.junit.Before; 
import org.junit.After; 

/** 
* InfluxDBV2 Tester. 
* 
* @author <Authors name> 
* @since <pre>五月 11, 2018</pre> 
* @version 1.0 
*/ 
public class InfluxDBV2Test { 

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
}

/** 
* 
* Method: init() 
* 
*/ 
@Test
public void testInit() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: registerSchema()
* 
*/ 
@Test
public void testCreateSchema() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: insertOneBatch(String device, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) 
* 
*/ 
@Test
public void testInsertOneBatchForDeviceBatchIndexTotalTimeErrorCount() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: insertOneBatch(LinkedList<String> cons, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) 
* 
*/ 
@Test
public void testInsertOneBatchForConsBatchIndexTotalTimeErrorCount() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: close() 
* 
*/ 
@Test
public void testClose() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: createDatabase(String databaseName) 
* 
*/ 
@Test
public void testCreateDatabase() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: main(String[] args) 
* 
*/ 
@Test
public void testMain() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: getTotalTimeInterval() 
* 
*/ 
@Test
public void testGetTotalTimeInterval() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: executeOneQuery(List<Integer> devices, int index, long startTime, QueryClientThread client, ThreadLocal<Long> errorCount) 
* 
*/ 
@Test
public void testExecuteOneQuery() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: count(String group, String device, String sensor) 
* 
*/ 
@Test
public void testCount() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: getSelectClause(int num, String method, boolean is_aggregate_fun, List<String> sensorList) 
* 
*/ 
@Test
public void testGetSelectClause() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: getPath(List<Integer> devices) 
* 
*/ 
@Test
public void testGetPath() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: insertOneBatchMulDevice(LinkedList<String> deviceCodes, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) 
* 
*/ 
@Test
public void testInsertOneBatchMulDevice() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: createSchemaOfDataGen() 
* 
*/ 
@Test
public void testCreateSchemaOfDataGen() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: insertGenDataOneBatch(String s, int i, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) 
* 
*/ 
@Test
public void testInsertGenDataOneBatch() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: exeSQLFromFileByOneBatch() 
* 
*/ 
@Test
public void testExeSQLFromFileByOneBatch() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: insertOverflowOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, ArrayList<Integer> before, Integer maxTimestampIndex, Random random) 
* 
*/ 
@Test
public void testInsertOverflowOneBatch() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: insertOverflowOneBatchDist(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, Integer maxTimestampIndex, Random random) 
* 
*/ 
@Test
public void testInsertOverflowOneBatchDist() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: getLabID() 
* 
*/ 
@Test
public void testGetLabID() throws Exception { 
//TODO: Test goes here... 
} 


/** 
* 
* Method: createDataModel(int batchIndex, int dataIndex, String device) 
* 
*/ 
@Test
public void testCreateDataModelForBatchIndexDataIndexDevice() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = InfluxDBV2.getClass().getMethod("createDataModel", int.class, int.class, String.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

/** 
* 
* Method: createDataModel(int timestampIndex, String device) 
* 
*/ 
@Test
public void testCreateDataModelForTimestampIndexDevice() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = InfluxDBV2.getClass().getMethod("createDataModel", int.class, String.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

/** 
* 
* Method: getDeviceNum(String device) 
* 
*/ 
@Test
public void testGetDeviceNum() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = InfluxDBV2.getClass().getMethod("getDeviceNum", String.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

/** 
* 
* Method: createQuerySQLStatment(List<Integer> devices, int num, long time, List<String> sensorList) 
* 
*/ 
@Test
public void testCreateQuerySQLStatmentForDevicesNumTimeSensorList() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = InfluxDBV2.getClass().getMethod("createQuerySQLStatment", List<Integer>.class, int.class, long.class, List<String>.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

/** 
* 
* Method: createQuerySQLStatment(List<Integer> devices, int num, List<String> sensorList) 
* 
*/ 
@Test
public void testCreateQuerySQLStatmentForDevicesNumSensorList() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = InfluxDBV2.getClass().getMethod("createQuerySQLStatment", List<Integer>.class, int.class, List<String>.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

/** 
* 
* Method: createQuerySQLStatment(List<Integer> devices, int num, String method, List<String> sensorList) 
* 
*/ 
@Test
public void testCreateQuerySQLStatmentForDevicesNumMethodSensorList() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = InfluxDBV2.getClass().getMethod("createQuerySQLStatment", List<Integer>.class, int.class, String.class, List<String>.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

/** 
* 
* Method: createQuerySQLStatment(List<Integer> devices, int num, String method, long startTime, long endTime, List<String> sensorList) 
* 
*/ 
@Test
public void testCreateQuerySQLStatmentForDevicesNumMethodStartTimeEndTimeSensorList() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = InfluxDBV2.getClass().getMethod("createQuerySQLStatment", List<Integer>.class, int.class, String.class, long.class, long.class, List<String>.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

/** 
* 
* Method: createQuerySQLStatment(List<Integer> devices, int num, long startTime, long endTime, List<String> sensorList) 
* 
*/ 
@Test
public void testCreateQuerySQLStatmentForDevicesNumStartTimeEndTimeSensorList() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = InfluxDBV2.getClass().getMethod("createQuerySQLStatment", List<Integer>.class, int.class, long.class, long.class, List<String>.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

/** 
* 
* Method: createQuerySQLStatment(List<Integer> devices, int num, long startTime, long endTime, Number value, List<String> sensorList) 
* 
*/ 
@Test
public void testCreateQuerySQLStatmentForDevicesNumStartTimeEndTimeValueSensorList() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = InfluxDBV2.getClass().getMethod("createQuerySQLStatment", List<Integer>.class, int.class, long.class, long.class, Number.class, List<String>.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

/** 
* 
* Method: createQuerySQLStatment(List<Integer> devices, String method, int num, long startTime, long endTime, Number value, List<String> sensorList) 
* 
*/ 
@Test
public void testCreateQuerySQLStatmentForDevicesMethodNumStartTimeEndTimeValueSensorList() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = InfluxDBV2.getClass().getMethod("createQuerySQLStatment", List<Integer>.class, String.class, int.class, long.class, long.class, Number.class, List<String>.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

} 
