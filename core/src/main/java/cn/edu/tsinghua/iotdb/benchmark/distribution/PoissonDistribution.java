package cn.edu.tsinghua.iotdb.benchmark.distribution;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import java.util.Random;

public class PoissonDistribution {
    private final Config config ;
    private double lambda;
    private final Random random;
    private int deltaKinds;
    private static final double BASIC_MODEL_LAMBDA = 10;
    private static final int BASIC_MODEL_MAX_K = 25;


    public void setLambda(double lambda) {
        this.lambda = lambda;
    }

    public void setDeltaKinds(int deltaKinds) {
        this.deltaKinds = deltaKinds;
    }

    public PoissonDistribution(Random ran) {
        this.config = ConfigDescriptor.getInstance().getConfig();
        this.random = ran;
        this.lambda = config.LAMBDA;
        this.deltaKinds = config.MAX_K;
    }

    private double getPossionProbability(int k, double la) {
        double c = Math.exp(-la);
        double sum = 1;
        for (int i = 1; i <= k; i++) {
            sum *= la / i;
        }
        return sum * c;
    }

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
            double[] p = new double[BASIC_MODEL_MAX_K];
            double sum = 0;
            for (int i = 0; i < BASIC_MODEL_MAX_K - 1; i++) {
                p[i] = getPossionProbability(i, BASIC_MODEL_LAMBDA);
                sum += p[i];
            }
            p[BASIC_MODEL_MAX_K - 1] = 1 - sum;
            double[] range = new double[BASIC_MODEL_MAX_K + 1];
            range[0] = 0;
            for (int i = 0; i < BASIC_MODEL_MAX_K; i++) {
                range[i + 1] = range[i] + p[i];
            }
            for (int i = 0; i < BASIC_MODEL_MAX_K; i++) {
                nextDelta++;
                if (isBetween(rand, range[i], range[i + 1])) {
                    break;
                }
            }
            double step;
            if(nextDelta <= BASIC_MODEL_LAMBDA){
                step = lambda / BASIC_MODEL_LAMBDA;
            }else{
                step =  (deltaKinds - lambda) / (BASIC_MODEL_MAX_K - BASIC_MODEL_LAMBDA) ;
            }
            nextDelta = (int)(lambda + ((nextDelta - BASIC_MODEL_LAMBDA) * step)) ;
        }
        return nextDelta;
    }

    private static boolean isBetween(double a, double b, double c) {
        return a > b && a < c;
    }

}
