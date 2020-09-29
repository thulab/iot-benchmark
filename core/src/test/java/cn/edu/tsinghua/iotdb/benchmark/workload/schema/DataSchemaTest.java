package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class DataSchemaTest {
  private static Config config = ConfigDescriptor.getInstance().getConfig();

  @Test
  public void test(){
    testBalanceSplit();
  }

  void testBalanceSplit(){
    int preDeviceNum = config.DEVICE_NUMBER;
    int preClientNum = config.CLIENT_NUMBER;
    config.DEVICE_NUMBER = 100;
    config.CLIENT_NUMBER = 30;
    int mod = config.DEVICE_NUMBER % config.CLIENT_NUMBER;
    int deviceNumEachClient = config.DEVICE_NUMBER / config.CLIENT_NUMBER;
    config.initDeviceCodes();
    DataSchema dataSchema = DataSchema.getInstance();
    Map<Integer, List<DeviceSchema>> client2Schema = dataSchema.getClientBindSchema();
    for (int clientId : client2Schema.keySet()){
      int deviceNumInClient = client2Schema.get(clientId).size();
      if ( clientId < mod){
        Assert.assertEquals(deviceNumEachClient+1,deviceNumInClient);
      }
      else {
        Assert.assertEquals(deviceNumEachClient,deviceNumInClient);
      }
    }
    config.DEVICE_NUMBER = preDeviceNum;
    config.CLIENT_NUMBER = preClientNum;
  }


}
