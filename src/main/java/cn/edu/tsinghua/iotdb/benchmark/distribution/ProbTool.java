package cn.edu.tsinghua.iotdb.benchmark.distribution;

import java.util.Random;

public class ProbTool {



    /**
     * 使用QUERY_SEED参数作为随机数种子
     *
     * @param p 返回true的概率
     * @return 布尔值
     */
    public boolean returnTrueByProb(double p, Random random) {
        if (random.nextDouble() < p) {
            return true;
        } else {
            return false;
        }
    }


}
