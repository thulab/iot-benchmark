package cn.edu.tsinghua.iotdb.benchmark.distribution;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * PoissonDistribution Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>五月 11, 2018</pre>
 */
public class PoissonDistributionTest {

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  /**
   * Method: setLambdaConfig(double lambda)
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
   * Method: getNextPoissonDelta()
   */
  //@Test
  public void testGetNextPoissonDelta() {
    ArrayList<Integer> list = new ArrayList<>();
    PoissonDistribution poissonDistribution = new PoissonDistribution(new Random(100));
    poissonDistribution.setDeltaKindsConfig(10);
    poissonDistribution.setLambdaConfig(5.0);
    for (int i = 0; i < 10; i++) {
      list.add(poissonDistribution.getNextPoissonDelta());
    }

    int[] count = new int[11];
    for (int a : list) {
      count[a]++;
    }

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
   * Method: getPoissonProbability(int k, double la)
   */
  @Test
  public void testGetPoissonProbability() throws Exception {
//TODO: Test goes here...
/*
try {
   Method method = PoissonDistribution.getClass().getMethod("getPossionProbability", int.class, double.class);
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
   Method method = PoissonDistribution.getClass().getMethod("isBetween", double.class, double.class, double.class);
   method.setAccessible(true);
   method.invoke(<Object>, <Parameters>);
} catch(NoSuchMethodException e) {
} catch(IllegalAccessException e) {
} catch(InvocationTargetException e) {
}
*/
  }

}

