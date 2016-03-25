import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer


/**
  * Created by liuh on 2016/3/25.
  * rbda using scala  run on spark
  */
object NewMethod {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NewMethod").setMaster("spark://liuh-pc:7077").setJars(List("/home/liuh/workspace/matrix-scala-spark/out/artifacts/matrix_scala_spark_jar/matrix_scala_spark.jar"))
    val sc = new SparkContext(conf)

    //定义 lb 手机random列
    val lb = new ListBuffer[String]

    for(i <- 1.to(500)){ //循环多少次收集多少次
      val randomArray = new Array[Int](16) // 收集所有139列中的16列
      for(i<-1.to(16)){
        val ii = Math.random().*(138).toInt+1 //列号2-139
        //testSet.add(ii)
        randomArray(i-1) = ii
      }
      val distArray = randomArray.distinct //去重复
      val sb = new StringBuilder
      for(value <- distArray)
        sb.append(value+" ")
      lb.append(sb.toString)
    }
    val result = lb.toList
    result.foreach(println(_))
    //println(result.toString)

    val lines = sc.parallelize(result)  // 这不就是 random initial index
    //lines.repartition(1).saveAsTextFile("hdfs://liuh-pc:9000/output/random1") 得到随机行组合
    val matrix = sc.textFile("hdfs://liuh-pc:9000/mllib/matrix01.txt").flatMap(_.split("\t")).map(_.toInt)

    //val result1 = matrix.map(_.split(" ")).collect

    val localArray = matrix.collect //所有数据组成一个大Array OopsOOM！！！！
    System.out.println("size: "+localArray.length)

    for(i <- 1 to 100)
      println(i+"  "+localArray(i))

    //val arrayMatrix = new Array[Array[Int]](139)
    val arrayMatrix = Array.ofDim[Int](139,1566) //构造二维数组
    for(i <- 0 to 138){
      for(j <- 0 to 1565){
        arrayMatrix(i)(j) = localArray(i*1566+j)
      }
    }
    val arrayY = new Array[Int](1566) //构造Y值数组
    for(i <- 0 to 1565){
      arrayY(i) = localArray(i)
    }
    //println("result: "+arrayMatrix) 测试测试
    var index = 0;
    for(i <- arrayMatrix){
      for(j <- i){
        println(index+"  "+j + "  "+localArray(index)) //对比矩阵，相等就对了 表示二维数组构造完成
        index += 1
      }
    }
    //测试测试
    var index2 = 0
    for(i<-arrayY){
      println(index2 + "  "+ i+ "  "+localArray(index2))
      index2 += 1
    }

    //函数，根据随机的index和arrayMatrix构造选中属性的矩阵 index 1-139 所以要见减去1
    val pickRow = (index :Array[Int]) => {
      val returnResult = Array.ofDim[Int](index.length,1566)
      for(i <- 1.to(index.length-1))
        returnResult(i) = arrayMatrix(index(i)-1)
      returnResult
    }
    //测试方法
    val text1 = pickRow(Array(1,2))

    for(i <- text1){
      for(j <- i){
        println(j) //对比矩阵，相等就对了 表示二维数组构造完成
      }
    }

    //lines RDD[Array]

    def string2int = (stringArray:Array[String]) =>{
      val intArray = new Array[Int](stringArray.length)
      for(i <- 0.to(stringArray.length-1))
        intArray(i) = stringArray(i).toInt
      intArray
    }
    //随机选取列的矩阵

    lines.map(_.split(" ")).map(string2int(_)).map(pickRow(_))

  }
}
