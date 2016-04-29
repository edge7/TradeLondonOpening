import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, IndexToString, VectorIndexer}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

/* Loading dataSet from Hive, using pre-built hiveContext */
val dataSet = sqlContext.sql(" select * from stratagem.eur_usd_2009_2015_2_classes")

val nFolds: Int = 10
val NumTrees: Int = 10

/* Since, RandomForestClassifier wants label and features as input columns,
   we need to create a transform for that
*/ 
val assembler = new VectorAssembler().setInputCols(Array("rsi", "hour", "low_1", "low_2", "low_3", "body_1", "body_2", "body_3", "high_1", "high_2", "high_3")).setOutputCol("features")

val dataSetLabelAndFeatures = assembler.transform(dataSet).select("label", "features").withColumnRenamed("label", "category")

val rf = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("indexedFeatures").setNumTrees(NumTrees)

// Automatically identify categorical features, and index them.
// Set maxCategories so features with > 4 distinct values are treated as continuous.
val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(24).fit(dataSetLabelAndFeatures)

val indexer = new StringIndexer().setInputCol("category").setOutputCol("label").fit(dataSetLabelAndFeatures)

// Convert indexed labels back to original labels.
val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(indexer.labels)

val pipeline = new Pipeline().setStages(Array(indexer, featureIndexer, rf, labelConverter)) 

val paramGrid = new ParamGridBuilder().addGrid(rf.numTrees, Array(100, 200, 300, 400) ).addGrid(rf.maxDepth, Array(5,10, 15,20,25)).build() // No parameter search

val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("weightedPrecision")
  // "f1", "precision", "recall", "weightedPrecision", "weightedRecall"

val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(nFolds)

val splits = dataSetLabelAndFeatures.randomSplit(Array(0.85, 0.15))
val (trainingData, testData) = (splits(0), splits(1))
trainingData.cache 
val startTraining = System.currentTimeMillis
val model = cv.fit(trainingData) 
val endTraining = System.currentTimeMillis

val timeToTrainInSec = (endTraining - startTraining) / 1000
val timeToTrainInMin = timeToTrainInSec / 60 

/* GET Best Parameter */
val bestPipelineModel = model.bestModel.asInstanceOf[PipelineModel]
val stages = bestPipelineModel.stages
val rfStage = stages(2).asInstanceOf[RandomForestClassificationModel]
val numTrees = rfStage.numTrees



/* After having got best Parameters through Cross-Fold, create a new model
   using those parameters
*/
val bestModel = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("indexedFeatures").setNumTrees(numTrees).setMaxDepth(15)
val pipeline = new Pipeline().setStages(Array(indexer, featureIndexer, bestModel, labelConverter)) 
val newModel = pipeline.fit(trainingData)
val testDataT =  newModel.transform(testData)
val predictionAndLabel =  testDataT.map( x=>  (x.getString(7).toDouble, x.getDouble(0) ))
val metrics = new BinaryClassificationMetrics(predictionAndLabels)
