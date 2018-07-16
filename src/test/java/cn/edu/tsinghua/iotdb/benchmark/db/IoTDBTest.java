package java.cn.edu.tsinghua.iotdb.benchmark.db; 

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.db.IDBFactory;
import cn.edu.tsinghua.iotdb.benchmark.db.IDatebase;
import cn.edu.tsinghua.iotdb.benchmark.db.IoTDB;
import cn.edu.tsinghua.iotdb.benchmark.db.IoTDBFactory;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import java.util.ArrayList;
import java.util.List;

/** 
* IoTDB Tester. 
* 
* @author <Authors name> 
* @since <pre>五月 11, 2018</pre> 
* @version 1.0 
*/ 
public class IoTDBTest {

    static List<Integer> devices ;
    static long startTime;
    static List<String> sensorList ;
    static  IoTDB ioTDB ;
    static private Config config;


@Before
public void before() throws Exception {
    config = ConfigDescriptor.getInstance().getConfig();
    ioTDB = new IoTDB(0);
    devices = new ArrayList<>();
    sensorList = new ArrayList<>();
    devices.add(0);
    devices.add(1);

} 

@After
public void after() throws Exception { 
}


private void printSQL(){

}

/** 
* 
* Method: createSchema() 
* 
*/ 
@Test
public void testCreateSchema() throws Exception { 
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
* Method: insertOneBatchMulDevice(LinkedList<String> deviceCodes, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) 
* 
*/ 
@Test
public void testInsertOneBatchMulDevice() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: insertOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) 
* 
*/ 
@Test
public void testInsertOneBatchForDeviceLoopIndexTotalTimeErrorCount() throws Exception { 
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
* Method: insertOneBatch(LinkedList<String> cons, int batchIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) 
* 
*/ 
@Test
public void testInsertOneBatchForConsBatchIndexTotalTimeErrorCount() throws Exception { 
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
* Method: init() 
* 
*/ 
@Test
public void testInit() throws Exception { 
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
* Method: createSQLStatment(int batch, int index, String device) 
* 
*/ 
@Test
public void testCreateSQLStatmentForBatchIndexDevice() throws Exception {
//TODO: Test goes here...
} 

/** 
* 
* Method: createSQLStatment(String device, int timestampIndex) 
* 
*/ 
@Test
public void testCreateSQLStatmentForDeviceTimestampIndex() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: createGenDataSQLStatment(int batch, int index, String device) 
* 
*/ 
@Test
public void testCreateGenDataSQLStatment() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: createQuerySQLStatment(List<Integer> devices, int num, long time, List<String> sensorList) 
* 
*/ 
@Test
public void testCreateQuerySQLStatmentForDevicesNumTimeSensorList() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: createQuerySQLStatment(List<Integer> devices, int num, List<String> sensorList) 
* 
*/ 
@Test
public void testCreateQuerySQLStatmentForDevicesNumSensorList() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: createQuerySQLStatment(List<Integer> devices, int num, String method, List<String> sensorList) 
* 
*/ 
@Test
public void testCreateQuerySQLStatmentForDevicesNumMethodSensorList() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: createQuerySQLStatment(List<Integer> devices, int num, String method, long startTime, long endTime, List<String> sensorList) 
* 
*/ 
@Test
public void testCreateQuerySQLStatmentForDevicesNumMethodStartTimeEndTimeSensorList() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: createQuerySQLStatment(List<Integer> devices, int num, long startTime, long endTime, List<String> sensorList) 
* 
*/ 
@Test
public void testCreateQuerySQLStatmentForDevicesNumStartTimeEndTimeSensorList() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: createQuerySQLStatment(List<Integer> devices, int num, long startTime, long endTime, Number value, List<String> sensorList, int limit_n, int offset) 
* 
*/ 
@Test
public void testCreateQuerySQLStatmentForDevicesNumStartTimeEndTimeValueSensorListLimit_nOffset() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: createQuerySQLStatment(int device_id, int limit_n, int offset, int series_limit, int series_offset) 
* 
*/ 
@Test
public void testCreateQuerySQLStatmentForDevice_idLimit_nOffsetSeries_limitSeries_offset() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: createQuerySQLStatment(List<Integer> devices, int num, long startTime, long endTime, Number value, List<String> sensorList) 
* 
*/ 
@Test
public void testCreateQuerySQLStatmentForDevicesNumStartTimeEndTimeValueSensorList() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: createQuerySQLStatment(List<Integer> devices, String method, int num, List<Long> startTime, List<Long> endTime, Number value, List<String> sensorList) 
* 
*/ 
@Test
public void testCreateQuerySQLStatmentForDevicesMethodNumStartTimeEndTimeValueSensorList() throws Exception { 
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
* Method: count(String group, String device, String sensor) 
* 
*/ 
@Test
public void testCount() throws Exception { 
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
* Method: insertGenDataOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) 
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
* Method: initSchema() 
* 
*/ 
@Test
public void testInitSchema() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = IoTDB.getClass().getMethod("initSchema"); 
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
* Method: string2num(String str) 
* 
*/ 
@Test
public void testString2num() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = IoTDB.getClass().getMethod("string2num", String.class); 
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
* Method: createTimeseries(String path, String sensor) 
* 
*/ 
@Test
public void testCreateTimeseries() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = IoTDB.getClass().getMethod("createTimeseries", String.class, String.class); 
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
* Method: createTimeseriesBatch(String path, String sensor, int count, int timeseriesTotal, Statement statement) 
* 
*/ 
@Test
public void testCreateTimeseriesBatch() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = IoTDB.getClass().getMethod("createTimeseriesBatch", String.class, String.class, int.class, int.class, Statement.class); 
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
* Method: setStorgeGroup(String device) 
* 
*/ 
@Test
public void testSetStorgeGroup() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = IoTDB.getClass().getMethod("setStorgeGroup", String.class); 
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
* Method: getDataByTypeAndScope(long currentTime, Config config) 
* 
*/ 
@Test
public void testGetDataByTypeAndScope() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = IoTDB.getClass().getMethod("getDataByTypeAndScope", long.class, Config.class); 
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
* Method: getRandomText(long currentTime, String text) 
* 
*/ 
@Test
public void testGetRandomText() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = IoTDB.getClass().getMethod("getRandomText", long.class, String.class); 
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
* Method: getRandomInt(long currentTime, String scope) 
* 
*/ 
@Test
public void testGetRandomInt() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = IoTDB.getClass().getMethod("getRandomInt", long.class, String.class); 
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
* Method: getRandomBoolean(long currentTime) 
* 
*/ 
@Test
public void testGetRandomBoolean() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = IoTDB.getClass().getMethod("getRandomBoolean", long.class); 
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
* Method: getRandomFloat(long currentTime, String scope) 
* 
*/ 
@Test
public void testGetRandomFloat() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = IoTDB.getClass().getMethod("getRandomFloat", long.class, String.class); 
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
* Method: createSQLStatmentOfMulDevice(int loopIndex, int i, String device) 
* 
*/ 
@Test
public void testCreateSQLStatmentOfMulDevice() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = IoTDB.getClass().getMethod("createSQLStatmentOfMulDevice", int.class, int.class, String.class); 
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
* Method: getGroupDevicePath(String device) 
* 
*/ 
@Test
public void testGetGroupDevicePath() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = IoTDB.getClass().getMethod("getGroupDevicePath", String.class); 
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
* Method: getFullGroupDevicePathByID(int id) 
* 
*/ 
@Test
public void testGetFullGroupDevicePathByID() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = IoTDB.getClass().getMethod("getFullGroupDevicePathByID", int.class); 
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
* Method: getTypeByField(String name) 
* 
*/ 
@Test
public void testGetTypeByField() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = IoTDB.getClass().getMethod("getTypeByField", String.class); 
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
* Method: writeSQLIntoFile(String sql, String gen_data_file_path) 
* 
*/ 
@Test
public void testWriteSQLIntoFile() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = IoTDB.getClass().getMethod("writeSQLIntoFile", String.class, String.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
} 

} 
