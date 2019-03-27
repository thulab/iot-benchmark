package cn.edu.tsinghua.iotdb.benchmark.distribution;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iotdb.benchmark.distribution.PossionDistribution;
import java.util.ArrayList;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * PossionDistribution Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>五月 11, 2018</pre>
 */
public class PossionDistributionTest {

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  /**
   * Method: setLambda(double lambda)
   */
  @Test
  public void testSetLambda() throws Exception {
//TODO: Test goes here...
  }

  /**
   * Method: setDeltaKinds(int deltaKinds)
   */
  @Test
  public void testSetDeltaKinds() throws Exception {
//TODO: Test goes here...
  }

  /**
   * Method: getNextPossionDelta()
   */
  @Test
  public void testGetNextPossionDelta() throws Exception {
    ArrayList<Integer> list = new ArrayList<>();
    PossionDistribution possionDistribution = new PossionDistribution(new Random(100));
    possionDistribution.setDeltaKinds(10);
    possionDistribution.setLambda(5.0);
    for (int i = 0; i < 1000; i++) {
      list.add(possionDistribution.getNextPossionDelta());
    }
    /*
    for(int a : list){
        System.out.println(a);
    }
    */
    int[] count = new int[11];
    for (int a : list) {
      count[a]++;
    }
    /*
    for(int a: count){
        System.out.println(a);
    }
    */
    assertEquals(0, count[0]);
    assertEquals(47, count[1]);
    assertEquals(152, count[2]);
    assertEquals(227, count[3]);
    assertEquals(232, count[4]);
    assertEquals(167, count[5]);
    assertEquals(89, count[6]);
    assertEquals(46, count[7]);
    assertEquals(28, count[8]);
    assertEquals(7, count[9]);
    assertEquals(5, count[10]);
  }


  /**
   * Method: getPossionProbability(int k, double la)
   */
  @Test
  public void testGetPossionProbability() throws Exception {
//TODO: Test goes here...
/*
try {
   Method method = PossionDistribution.getClass().getMethod("getPossionProbability", int.class, double.class);
   method.setAccessible(true);
   method.invoke(<Object>, <Parameters>);
} catch(NoSuchMethodException e) {
} catch(IllegalAccessException e) {
} catch(InvocationTargetException e) {
}
*/
  }

  /**
   * Method: isBetween(double a, double b, double c)
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

}

