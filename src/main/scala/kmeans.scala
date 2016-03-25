import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.Vectors


/**
  * Created by liuh on 2016/3/24.
  */

object kmeans {
  def main(args: Array[String]) {
    //屏蔽不用的日志在终端显示
    Logger.getLogger("org.apache.saprk").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("kmeans").setMaster("spark://liuh-pc:7077").setJars(List("/home/liuh/workspace/class3/out/artifacts/class3_jar/class3.jar"))
    val sc = new SparkContext(conf)

    //装载数据集
    val data = sc.textFile("hdfs://liuh-pc:9000/mllib/kmeans_data.txt")
    val parseData = data.map(s=> Vectors.dense(s.split(' ').map(_.toDouble)))

    //讲数据集聚类，2个类，20次迭代，进行模型训练成数据类型
    val numClusters = 2
    val numIterations = 20
    val module = KMeans.train(parseData,numClusters,numIterations)

    //打印数据模型中心点
    println("Cluster centers: ")
    for(c<-module.clusterCenters){
      println(" "+c.toString)
    }

    //使用误差平方之和来评估数据模型
    val cost = module.computeCost(parseData)
    println("within set sum of squared errors = "+cost)

    //使用模型测试单点数据
    println("Vectors 0.2 0.2 0.2 is belongs to cluster: "+module.predict(Vectors.dense("0.2 0.2 0.2".split(' ').map(_.toDouble))))
    println("Vectors 0.25 0.25 0.25 is belongs to cluster: "+module.predict(Vectors.dense("0.25 0.25 0.25".split(' ').map(_.toDouble))))
    println("Vectors 8 8 8 is belongs to cluster: "+module.predict(Vectors.dense("8 8 8".split(' ').map(_.toDouble))))

    //交叉评估1，至返回结果
    val testdata = data.map(s=> Vectors.dense(s.split(' ').map(_.toDouble)))
    val result1 = module.predict(testdata)
    result1.saveAsTextFile("hdfs://liuh-pc:9000/output/mllibtest1")

    //交叉评估2，返回数据集和结果
    val result2 =data.map{
      line =>
        val linevectore = Vectors.dense(line.split(" ").map(_.toDouble))
        val prediction = module.predict(linevectore)
        line + " " + prediction
    }.saveAsTextFile("hdfs://liuh-pc:9000/output/mllibtest1")

    sc.stop()


  }
}
