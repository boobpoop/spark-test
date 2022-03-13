package spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount_Spark01 {
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


    //3. 将数据根据单词进行分组，便于统计
    // (hello, hello), (world, world)
    // wordGroup是一个元组，key是分组的单词，value是同一组的所有单词。例如，以world进行分组的数据，world作为key，value就是(world, world)
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    //4. 对分组后的数据进行转换
    // (hello, hello), (world, world) -> (hello, 2), (world, 2)
    val wordToGroup: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    //5. 将转换结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordToGroup.collect()
    array.foreach(println)

    //关闭连接
    sc.stop()
  }
}
