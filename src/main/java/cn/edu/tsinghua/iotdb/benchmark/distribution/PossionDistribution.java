package cn.edu.tsinghua.iotdb.benchmark.distribution;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class PossionDistribution {
    private static final Logger LOGGER = LoggerFactory.getLogger(PossionDistribution.class);
    private Config config ;
    private double lambda;
    private Random random;
    private int deltaKinds;

    public void setLambda(double lambda) {
        this.lambda = lambda;
    }

    public void setDeltaKinds(int deltaKinds) {
        this.deltaKinds = deltaKinds;
    }

    public PossionDistribution(Random ran) {
        this.config = ConfigDescriptor.getInstance().getConfig();
        this.lambda = config.LAMBDA;
        this.deltaKinds = config.MAX_K;
        this.random = ran;
    }

    private double getPossionProbability(int k) {
        double p;
        if (k < 21) {

            long factorK = factor(k);
            p = Math.pow(lambda, k) * Math.pow(Math.E, -lambda) / factorK;
        } else {
            k = 20;
            long factorK = factor(k);
            p = Math.pow(lambda, k) * Math.pow(Math.E, -lambda) / factorK;
            LOGGER.error("k of possion distribution should be no more than 20!");
        }
        return p;
    }

    public int getNextPossionDelta() {
        int nextDelta = 0;
        double rand = random.nextDouble();
        double[] p = new double[deltaKinds];
        double sum = 0;
        for (int i = 0; i < deltaKinds - 1; i++) {
            p[i] = getPossionProbability(i);
            sum += p[i];
        }
        p[deltaKinds - 1] = 1 - sum;
        double[] range = new double[deltaKinds + 1];
        range[0] = 0;
        for (int i = 0; i < deltaKinds; i++) {
            range[i + 1] = range[i] + p[i];
        }
        for (int i = 0; i < deltaKinds; i++) {
            nextDelta++;
            if (isBetween(rand, range[i], range[i + 1])) {
                break;
            }
        }
        return nextDelta;
    }

    private static boolean isBetween(double a, double b, double c) {
        if (a > b && a < c) {
            return true;
        }
        return false;
    }

    private long factor(int n) {
        long num = 1;
        for (int i = 1; i <= n; i++) {
            num *= i;
        }
        return num;
    }
}
