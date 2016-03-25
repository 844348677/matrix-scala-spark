import breeze.numerics.ceil
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.{Matrices, Matrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by liuh on 2016/3/24.
  */
object BigHeart {
  def main(args: Array[String]) {
    //connect 信息
    val conf = new SparkConf().setAppName("BigHeart").setMaster("spark://liuh-pc:7077").setJars(List("/home/liuh/workspace/class3/out/artifacts/class3_jar/class3.jar"))
    val sc = new SparkContext(conf)

    val line = sc.textFile("hdfs://liuh-pc:9000/mllib/matrix01.txt").flatMap(_.split("\t")).map(_.toDouble)//.map(testFunc(_))//.map(x=>testFunc(x))
    println("size:"+line.count())
    //val fun = ceil _ //从技术上讲 _将ceil方法转成函数，我么可以直接调用函数，而不能直接调用方法
    val double3 = (x:Double) => 3*x
    def double33(x:Double) = 3*x
    def needFun(f:Double => Int) = f(0.25)
    val line2 = line.map(testFunc)

    sc.parallelize(Seq())
    //val testMatrix = line.map(m=>MatrixEntry(1,1,2))

   // println(testMatrix.getClass)

    // 139*1566 = 217674 转成矩阵
    val matrix : Matrix = Matrices.dense(139,1566,line.collect)
    println(matrix.numRows+" "+matrix.numCols)

    //val entries : RDD[MatrixEntry] = testMatrix

    //Array(1,2,3,4).map(fun)

  }
  def testFunc(s:Double): Double ={
    val x = s + 1
    x
  }
}
