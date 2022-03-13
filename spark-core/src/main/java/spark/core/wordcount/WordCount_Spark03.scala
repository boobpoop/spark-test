package spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount_Spark03 {
  def main(args: Array[String]): Unit = {
    //设置Spark框架的配置信息
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    //建立与Spark框架的连接
    val sc = new SparkContext(sparkConf)
    //执行业务操作

    //1. 读取文件，获取一行一行的数据
    // hello world
    val lines: RDD[String] = sc.textFile("wordcount/wordcount.txt")

    //2. 将一行数据进行拆分，形成一个一个的单词，这个过程就是分词，又称扁平映射
    // "hello world" -> hello, world, hello, world
    // 每行通过空格符切分每个单词
    val words: RDD[String] = lines.flatMap(_.split(" "))


    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))

    //3. 聚合
    //reduceByKey：通过的key数据，对value进行reduce聚合，可有三种写法
    //val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey((x, y) => {x + y})
    //val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey((x, y) => x + y)
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)


    //4. 将转换结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    //关闭连接
    sc.stop()
  }
}
