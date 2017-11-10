package cn.edu.tsinghua.iotdb.benchmark.loadData;

import java.util.LinkedList;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.benchmark.App;

public class Storage {
	private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
    private Config config;
	// 仓库最大存储量  
    private final int MAX_SIZE;
  
    // 仓库存储的载体  
    private volatile LinkedList<String> list;
    
    //已经生产的产品数目
    private volatile int storagedProductNum;
    
    //生产者线程是否结束
    private volatile boolean isEnd;
    
    public Storage() {
    	list = new LinkedList<String>();
        config = ConfigDescriptor.getInstance().getConfig();
    	storagedProductNum = 0;
        MAX_SIZE = config.BATCH_OP_NUM*config.DEVICE_NUMBER*4;
    	isEnd = false;
    }

	// 生产num个产品  
    public void produce(int num,LinkedList<String> prod)
    {  
        // 同步代码段  
        synchronized (list)  
        {  
            // 如果仓库剩余容量不足  
            while (list.size() + num > MAX_SIZE)  
            {  
                try  
                {  
                    // 由于条件不满足，生产阻塞  
                    list.wait();
                }  
                catch (InterruptedException e)  
                {  
                    e.printStackTrace();  
                }  
            }  
  
            // 生产条件满足情况下，生产num个产品  
            for (int i = 0; i < num; ++i)  
            {  
            	list.add(prod.removeFirst()); 
            }  
            storagedProductNum += num;
            list.notifyAll();  
        }  
    }  
  
    // 消费num个产品  
    public LinkedList<String> consume(int num)  
    {  
        // 同步代码段  
        synchronized (list)  
        {  
            // 如果仓库存储量不足  
            while (list.size() < num)
            {   
            	if(isEnd){
            		num = list.size();
            		break;
            	}
                try  
                {  
                    // 由于条件不满足，消费阻塞  
                    list.wait();  
                }  
                catch (InterruptedException e)  
                {  
                    e.printStackTrace();  
                }  
            } 
            
            LinkedList<String> cons = new LinkedList<String>();
            // 消费条件满足情况下，消费num个产品  
            for (int i = 0; i < num; ++i)  
            {  
            	String tmp = list.removeFirst(); 
            	cons.add(tmp);
            }  
            
            //storagedProductNum -= cons.size();
            list.notifyAll();  
            return cons; 
        } 
    }  
  
    // get/set方法  
    public synchronized LinkedList<String> getList()  
    {  
        return list;  
    }  
  
    public void setList(LinkedList<String> list)  
    {  
        this.list = list;  
    }  
  
    public int getMAX_SIZE()  
    {  
        return MAX_SIZE;  
    }
    
    public synchronized int getStoragedProductNum() {
		return storagedProductNum;
	}

	public void setStoragedProductNum(int storagedProductNum) {
		this.storagedProductNum = storagedProductNum;
	}

	public synchronized boolean isEnd() {
		return isEnd;
	}

	public void setEnd(boolean isEnd) {
		this.isEnd = isEnd;
	}
}

