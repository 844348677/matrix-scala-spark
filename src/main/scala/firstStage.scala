/**
  * Created by liuh on 2016/3/24.
  */

object firstStage {

/*  def main(args:Array[String]): Unit ={

    //.setDriver()
    val conf = new SparkConf().setAppName("firstStage").setMaster("spark://liuh-pc:7077").setJars(List("/home/liuh/workspace/class3/out/artifacts/class3_jar/class3.jar"))
    val sc = new SparkContext(conf)

    val lines = sc.textFile("hdfs://liuh-pc:9000/input/example.txt")
    val result = lines.flatMap(_.split(",")).map(x=>(x,1)).reduceByKey(_+_)

    result.repartition(1).saveAsTextFile("hdfs://liuh-pc:9000/inIdea/")
  }*/

}
