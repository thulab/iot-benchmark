package cn.edu.tsinghua.iotdb.benchmark.db;

public interface IDatabase {

  void init();

  void cleanup();

  void close();

  void createSchema();

  void insertOneBatch();

  void preciseQuery();

  void rangeQuery();

  void valueRangeQuery();

  void aggRangeQuery();

  void aggValueQuery();

  void aggRangeValueQuery();

  void groupByQuery();

  void latestPointQuery();

}
