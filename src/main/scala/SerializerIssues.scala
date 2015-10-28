import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ben on 24/06/15.
  */
object Spark {
  val ctx = new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]"))
}

object SerializerIssues extends App {
  new Test().doIT
}

class Test {
  val rddList = Spark.ctx.parallelize(List(1,2,3))

  def doIT() =  {
    val after = rddList.map(someFunc)
    after.collect().foreach(println)
  }

  def someFunc(a: Int) = a + 1
  //  val someFunc = (a: Int) => a + 1
}



