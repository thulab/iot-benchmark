package test.cn.edu.tsinghua.iotdb.benchmark.distribution; 

import cn.edu.tsinghua.iotdb.benchmark.distribution.PossionDistribution;
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
    assertEquals(5,count[1]);
    assertEquals(35,count[2]);
    assertEquals(84,count[3]);
    assertEquals(137,count[4]);
    assertEquals(182,count[5]);
    assertEquals(183,count[6]);
    assertEquals(148,count[7]);
    assertEquals(94,count[8]);
    assertEquals(61,count[9]);
    assertEquals(71,count[10]);

} 


/** 
* 
* Method: getPossionProbability(int k) 
* 
*/ 
@Test
public void testGetPossionProbability() throws Exception { 
//TODO: Test goes here... 
/* 
try { 
   Method method = PossionDistribution.getClass().getMethod("getPossionProbability", int.class); 
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

} 
