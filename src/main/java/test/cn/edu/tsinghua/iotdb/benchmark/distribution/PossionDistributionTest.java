package test.cn.edu.tsinghua.iotdb.benchmark.distribution; 

import cn.edu.tsinghua.iotdb.benchmark.distribution.PossionDistribution;
import cn.edu.tsinghua.iotdb.benchmark.sersyslog.OpenFileNumber;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import java.util.ArrayList;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/** 
* PossionDistribution Tester. 
* 
* @author liurui
* @since <pre>一月 30, 2018</pre> 
* @version 1.0 
*/ 
public class PossionDistributionTest { 

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: getNextPossionDelta() 
* 
*/ 
@Test
public void testGetNextPossionDelta() throws Exception { 
//TODO: Test goes here...
    ArrayList<Integer> list = new ArrayList<>();
    PossionDistribution possionDistribution = new PossionDistribution(new Random(100));
    possionDistribution.setDeltaKinds(10);
    possionDistribution.setLambda(5.0);
    for(int i=0;i<1000;i++){
        list.add(possionDistribution.getNextPossionDelta());
    }
    /*
    for(int a : list){
        System.out.println(a);
    }
    */
    int[] count = new int[11];
    for(int a : list){
        count[a] ++;
    }
    /*
    for(int a: count){
        System.out.println(a);
    }
    */
    assertEquals(0,count[0]);
    assertEquals(47,count[1]);
    assertEquals(152,count[2]);
    assertEquals(227,count[3]);
    assertEquals(232,count[4]);
    assertEquals(167,count[5]);
    assertEquals(89,count[6]);
    assertEquals(46,count[7]);
    assertEquals(28,count[8]);
    assertEquals(7,count[9]);
    assertEquals(5,count[10]);
} 


/** 
* 
* Method: getPossionProbability(int k) 
* 
*/ 
@Test
public void testGetPossionProbability() throws Exception {
    /*
    int testSize = 10000;
    int deltaKinds = 3000;
    ArrayList<Integer> list = new ArrayList<>();
    PossionDistribution possionDistribution = new PossionDistribution(new Random(100));
    possionDistribution.setDeltaKinds(deltaKinds);
    possionDistribution.setLambda(800.0);
    for(int i=0;i<testSize;i++){
        list.add(possionDistribution.getNextPossionDelta());
    }

    /*
    for(int a : list){
        System.out.println(a);
    }


    int[] count = new int[deltaKinds+1];
    for(int a : list){
        count[a] ++;
    }
    for(int i=0; i<deltaKinds+1 ;i++){
        System.out.print(count[i] + ",");
    }
    System.out.println();
    for(int i=0;i<deltaKinds+1;i++){
        System.out.print(i + ",");
    }
    */
} 

/** 
* 
* Method: isBetween(double a, double b, double c) 
* 
*/ 
@Test
public void testIsBetween() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = PossionDistribution.getClass().getMethod("isBetween", double.class, double.class, double.class); 
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
* Method: factor(int n) 
* 
*/ 
@Test
public void testFactor() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = PossionDistribution.getClass().getMethod("factor", int.class); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/ 
}

    public static void main(String[] args) {
        int loop = 2;
        int count = 0;
        System.out.println("TotalFiles  DataAndWalFiles  Sockets");
        while (count < loop) {
            count++;
            ArrayList<Integer> fileList = OpenFileNumber.getInstance().get();
            System.out.println(fileList.get(0) + "		" + fileList.get(1)  + "		" + fileList.get(2));
            try {
                Thread.sleep(1 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
        System.out.println("END!");
    }

} 
