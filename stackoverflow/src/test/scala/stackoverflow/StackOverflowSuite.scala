package stackoverflow

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

import stackoverflow.StackOverflow._

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

    override def langSpread = 50000

    override def kmeansKernels = 45

    override def kmeansEta: Double = 20.0D

    override def kmeansMaxIterations = 120
  }

  val lines = sc.textFile(getClass().getResource("/stackoverflow/stackoverflow.csv").getFile)
  val raw = testObject.rawPostings(lines)
  val grouped = testObject.groupedPostings(raw).cache

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("scoredPostings works") {
    val scored = scoredPostings(grouped)

    val testValues = Set((Posting(1, 6, None, None, 140, Some("CSS")), 67),
      (Posting(1, 42, None, None, 155, Some("PHP")), 89),
      (Posting(1, 72, None, None, 16, Some("Ruby")), 3),
      (Posting(1, 126, None, None, 33, Some("Java")), 30),
      (Posting(1, 174, None, None, 38, Some("C#")), 20))

    val result = scored.filter(testValues.contains(_)).collect

    assert(result.length == testValues.size, "scoredPostings does not find some expected values")
  }

  test("vectorPostings works") {
    val scored = scoredPostings(grouped)
    val vectors = vectorPostings(scored).cache

    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    val testValues = Set((350000, 67),
      (100000, 89),
      (300000, 3),
      (50000, 30),
      (200000, 20))

    val result = vectors.filter(testValues.contains(_)).distinct().collect
    assert(result.length == testValues.size, "vectorPostings does not find some expected values")
  }
}
