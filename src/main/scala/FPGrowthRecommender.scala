import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Ben on 24/06/15.
 */
object FPGrowthRecommender extends App {

  val sparkConf = new SparkConf().setMaster("local").setAppName("Napstify")

  val sparkContext = new SparkContext(sparkConf)

  val lines: RDD[String] = sparkContext.textFile("..//KdG-BigData/src/main/resources/distinctTweetsArtist.txt")
  val filteredLines = lines.filter(!_.toLowerCase.contains("fifth harmony"))
  val tweets: RDD[(String, String)] = filteredLines.map(line => {
    val splits = line.split("\t", 2)
    (splits(0), splits(1))
  })

  private val emptyTweetsFiltered: RDD[(String, String)] = tweets.filter(line => line._2.length > 0)

  private val artistsByUser: RDD[(String, Iterable[String])] = emptyTweetsFiltered.groupByKey()
  private val artistSets: RDD[Array[String]] = artistsByUser.map(values => values._2.toArray)

  val support = 5.0 / artistSets.count()
  println("Using " + support + " as support")

  val fpg = new FPGrowth()
    .setMinSupport(support)
  val model = fpg.run(artistSets)

  val minConfidence = 0.4
  model.generateAssociationRules(minConfidence).collect().sortBy(_.confidence).foreach { rule =>

    println(
      rule.antecedent.mkString("[", ",", "]")
        + " => " + rule.consequent.mkString("[", ",", "]")
        + ", " + rule.confidence)
  }


}
