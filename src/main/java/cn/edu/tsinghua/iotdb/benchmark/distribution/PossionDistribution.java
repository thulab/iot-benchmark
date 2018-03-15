package cn.edu.tsinghua.iotdb.benchmark.distribution;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Random;

public class PossionDistribution {
    private static final Logger LOGGER = LoggerFactory.getLogger(PossionDistribution.class);
    private Config config ;
    private double lambda;
    private Random random;
    private int deltaKinds;
    private final double basicModelLambda = 10;
    private final int basicModelMaxK = 25;


    public void setLambda(double lambda) {
        this.lambda = lambda;
    }

    public void setDeltaKinds(int deltaKinds) {
        this.deltaKinds = deltaKinds;
    }

    public PossionDistribution(Random ran) {
        this.config = ConfigDescriptor.getInstance().getConfig();
        this.random = ran;
        this.lambda = config.LAMBDA;
        this.deltaKinds = config.MAX_K;
    }

    private double getPossionProbability(int k, double la) {
        double c = Math.exp(-la), sum = 1;
        for (int i = 1; i <= k; i++) {
            sum *= la / i;
        }
        return sum * c;
    }

    /*
    public int getPossionVariable() {
        int x = 0;
        double y = random.nextDouble();
        double cdf = getPossionProbability(x);
        while (cdf < y) {
            x++;
            cdf += getPossionProbability(x);
        }
        return x;
    }
    */

    public int getNextPossionDelta() {
        int nextDelta = 0;
        if(lambda < 500) {
            double rand = random.nextDouble();
            double[] p = new double[config.MAX_K];
            double sum = 0;
            for (int i = 0; i < config.MAX_K - 1; i++) {
                p[i] = getPossionProbability(i,config.LAMBDA);
                sum += p[i];
            }
            p[config.MAX_K - 1] = 1 - sum;
            double[] range = new double[config.MAX_K + 1];
            range[0] = 0;
            for (int i = 0; i < config.MAX_K; i++) {
                range[i + 1] = range[i] + p[i];
            }
            for (int i = 0; i < config.MAX_K; i++) {
                nextDelta++;
                if (isBetween(rand, range[i], range[i + 1])) {
                    break;
                }
            }
        }else{
            double rand = random.nextDouble();
            double[] p = new double[basicModelMaxK];
            double sum = 0;
            for (int i = 0; i < basicModelMaxK - 1; i++) {
                p[i] = getPossionProbability(i,basicModelLambda);
                sum += p[i];
            }
            p[basicModelMaxK - 1] = 1 - sum;
            double[] range = new double[basicModelMaxK + 1];
            range[0] = 0;
            for (int i = 0; i < basicModelMaxK; i++) {
                range[i + 1] = range[i] + p[i];
            }
            for (int i = 0; i < basicModelMaxK; i++) {
                nextDelta++;
                if (isBetween(rand, range[i], range[i + 1])) {
                    break;
                }
            }
            double step;
            if(nextDelta <= basicModelLambda){
                step = lambda /basicModelLambda  ;
            }else{
                step =  (deltaKinds - lambda) / (basicModelMaxK - basicModelLambda) ;
            }
            nextDelta = (int)(lambda + ((nextDelta - basicModelLambda) * step)) ;
        }
        return nextDelta;
    }

    private static boolean isBetween(double a, double b, double c) {
        if (a > b && a < c) {
            return true;
        }
        return false;
    }

}
