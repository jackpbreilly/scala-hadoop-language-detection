import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object SimpleApp {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Simple Application")
		val sc = new SparkContext(conf)
		
		// input text
		val text = args(0)

		// Book Text
		val wordCountRDD = sc.textFile("hdfs:///user/cloudera/input_dir/"+text+".txt").
			flatMap( _.split("""[\s,.;:!?]+""") ).
			map( _.toLowerCase ).
			map( (_, 1) ).
			reduceByKey( _ + _ ).
			sortBy( z => (z._2, z._1), ascending = false )
  
		// English Text 
		val englishWordsRDD = sc.textFile("hdfs:///user/cloudera/englishwords").
			flatMap( _.split("""[\s,.;:!?]+""") ).
			map( _.toLowerCase ).
			map( (_, 1) ).
			reduceByKey( _ + _ ).
			sortBy( z => (z._2, z._1), ascending = false )	

		// French Text		
		val frenchWordsRDD = sc.textFile("hdfs:///user/cloudera/frenchwords").
			flatMap( _.split("""[\s,.;:!?]+""") ).
			map( _.toLowerCase ).
			map( (_, 1) ).
			reduceByKey( _ + _ ).
			sortBy( z => (z._2, z._1), ascending = false )

		// german Text		
		val germanWordsRDD = sc.textFile("hdfs:///user/cloudera/germanwords").
			flatMap( _.split("""[\s,.;:!?]+""") ).
			map( _.toLowerCase ).
			map( (_, 1) ).
			reduceByKey( _ + _ ).
			sortBy( z => (z._2, z._1), ascending = false )

		// Merges on Key and Sums Value 
		val mergedTextEnglish = (wordCountRDD ++ englishWordsRDD).groupBy(_._1).mapValues(_.map(_._2.asInstanceOf[Number].intValue()).sum)
		val mergedTextFrench = (wordCountRDD ++ frenchWordsRDD).groupBy(_._1).mapValues(_.map(_._2.asInstanceOf[Number].intValue()).sum)
		val mergedTextGerman = (wordCountRDD ++ germanWordsRDD).groupBy(_._1).mapValues(_.map(_._2.asInstanceOf[Number].intValue()).sum)

		// Calculating total unique language words
		var sumOfMergedEnglish=0
		mergedTextEnglish.collect.foreach{
			case (a, b) => if (b.toInt>1) sumOfMergedEnglish = sumOfMergedEnglish + b.toInt
		}

		var sumOfMergedFrench=0
		mergedTextFrench.collect.foreach{
			case (a, b) => if (b.toInt>1) sumOfMergedFrench = sumOfMergedFrench + b.toInt
		}

		var sumOfMergedGerman=0
		mergedTextGerman.collect.foreach{
			case (a, b) => if (b.toInt>1) sumOfMergedGerman = sumOfMergedGerman + b.toInt
		}

		// Calculating total words in book text
		var sumOfBookText=0
		wordCountRDD.collect.foreach{
			case (a, b) => if (b.toInt>1) sumOfBookText = sumOfBookText + b.toInt
		}

		// Print words which appear more than once
		println("Book Text Words: " + sumOfBookText)
		
		// Print unique words for each language
		println("English Words: " + sumOfMergedEnglish)
		println("French Words: " + sumOfMergedFrench)
		println("German Words: " + sumOfMergedGerman)

		// Print best guess for language
		if ((sumOfMergedEnglish > sumOfMergedFrench) && (sumOfMergedEnglish > sumOfMergedGerman)) println ("Best Guess: English")
		if ((sumOfMergedFrench > sumOfMergedEnglish) && (sumOfMergedFrench > sumOfMergedGerman)) println ("Best Guess: French")
		if ((sumOfMergedGerman > sumOfMergedFrench) && (sumOfMergedGerman > sumOfMergedEnglish)) println ("Best Guess: German")
}}
