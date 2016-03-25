import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by liuh on 2016/3/24.
  */
object randomList {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("randomList").setMaster("spark://liuh-pc:7077").setJars(List("/home/liuh/workspace/matrix-scala-spark/out/artifacts/matrix_scala_spark_jar"))
    val sc = new SparkContext(conf)

   // val lb = new ListBuffer[Array[Int]]
    val sb = new StringBuilder

    //val testSet  = new util.TreeSet[Int];
    for(i <- 1.to(500)){
      val randomArray = new Array[Int](16)
      for(i<-1.to(16)){
        val ii = Math.random().*(138).toInt+1 //2-139
        //testSet.add(ii)
        randomArray(i-1) = ii
      }

      val distArray = randomArray.distinct
      //println(":::"+distArray.size+" ")
     // distArray.foreach(x => print(x+" "))
      distArray.foreach(x => sb.append(x+" "))
      sb.append("\r\n")
  //    println()
    //  lb.append(distArray)
    }
    //val result = lb.toList
    println(sb.toString)
   // println(result.size)

    //val lines = sc.parallelize(sb.toString())
    //lines.repartition(1).saveAsTextFile("hdfs://liuh-pc:9000/output/random3")


  }
}
