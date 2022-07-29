package wordcount
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object Count {
  def main(args: Array[String]) = {
     val conf = new SparkConf()
      .setAppName("WordCount")
      println("df")
      var sc= new SparkContext(conf)
      var doc=sc.textFile("/home/hariharan/dataset/small_ds/lst")
      doc.saveAsTextFile("/home/hariharan/outputFromSubmitJarSpark")
      
}
}
