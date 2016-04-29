import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Gini
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel

// Load and parse the data file.

val data = sc.textFile("/home/edge7/Desktop/LO/TradeLondonOpening/EURUSD_2009To2015_1H_Model")
val parsedData = data.map { line =>
  val parts = line.split(',').map(_.toDouble)
  LabeledPoint(parts(0), Vectors.dense(parts.tail))
}
// Split the data into training and test sets (25% held out for testing)
val splits = parsedData.randomSplit(Array(0.75, 0.25))
val (trainingData, testData) = (splits(0), splits(1))

// Train a RandomForest model.
// Empty categoricalFeaturesInfo indicates all features are continuous.
val numClasses = 3
val categoricalFeaturesInfo = Map[Int, Int]()
val numTrees = 300 // Use more in practice.
val featureSubsetStrategy = "auto" // Let the algorithm choose.
val impurity = "gini"
val maxDepth = 13
val maxBins = 1024

val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
  numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

// Evaluate model on test instances and compute test error
val labelAndPreds = testData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val testPer = labelAndPreds.filter(r => r._1 == r._2).count.toDouble / testData.count()

//println("Learned classification tree model:\n" + model.toDebugString)

val tot_1 = labelAndPreds.filter(r => r._1 == 1).count.toDouble
val tot_2 = labelAndPreds.filter(r => r._1 == 2).count.toDouble
val tot_0 = labelAndPreds.filter(r => r._1 == 0).count.toDouble
val ok_1 = labelAndPreds.filter(r => r._1 == 1 && r._2 == 1).count.toDouble
val dico_1 = labelAndPreds.filter(r => r._2 == 1).count.toDouble

ok_1/dico_1

val ok_2 = labelAndPreds.filter(r => r._1 == 2 && r._2 == 2).count.toDouble
val dico_2 = labelAndPreds.filter(r => r._2 == 2).count.toDouble

ok_2/dico_2

val ok_0 = labelAndPreds.filter(r => r._1 == 0 && r._2 == 0).count.toDouble
val dico_0 = labelAndPreds.filter(r => r._2 == 0).count.toDouble

ok_0/dico_0

println("Test performance Totale = " + testPer)
val sellSignalsButBuy = labelAndPreds.filter(r => r._1 == 1 && r._2 == 2).count.toDouble/dico_2
val sellSignalsOk = labelAndPreds.filter(r => r._1 == 2 && r._2 == 2).count.toDouble/dico_2
val sellSignalButOut = labelAndPreds.filter(r => r._1 == 0 && r._2 == 2).count.toDouble/dico_2

val BuySignalsButSell = labelAndPreds.filter(r => r._1 == 2 && r._2 == 1).count.toDouble/dico_1
val BuySignalsButOut = labelAndPreds.filter(r => r._1 == 0 && r._2 == 1).count.toDouble/dico_1
val BuySignalsOk = labelAndPreds.filter(r => r._1 == 1 && r._2 == 1).count.toDouble/dico_1

val outSignalsOk = labelAndPreds.filter(r => r._1 == 0 && r._2 == 0).count.toDouble/dico_0


println("Test performance Totale = " + testPer)
sellSignalsButBuy 
sellSignalsOk 
sellSignalButOut
BuySignalsButSell 
BuySignalsButOut 
BuySignalsOk 
outSignalsOk 
