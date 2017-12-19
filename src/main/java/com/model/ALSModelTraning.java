package com.model;


import com.dto.Click;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.mllib.regression.LabeledPoint;


import org.apache.spark.ml.recommendation.ALSModel;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


public class ALSModelTraning {
    private transient SparkContext sc;
    private JavaSparkContext jsc;
    transient SparkConf conf;

    JavaRDD<LabeledPoint> productdata;

    JavaRDD<LabeledPoint> training;
    JavaRDD<LabeledPoint> test;


//    public MatrixFactorizationModel traningALS(JavaRDD <Rating> rdd,JavaRDD<Rating> validateData_Rating){
//        List<Integer> ranks = new ArrayList<Integer>();
//        ranks.add(8);
//        ranks.add(22);
//
//        List<Double> lambdas = new ArrayList<Double>();
//        lambdas.add(0.1);
//        lambdas.add(10.0);
//
//        List<Integer> iters = new ArrayList<Integer>();
//        iters.add(5);
//        iters.add(10);
//
//        MatrixFactorizationModel bestModel = null;
//        double bestValidateRnse = Double.MAX_VALUE;
//        int bestRank = 0;
//        double bestLambda = -1.0;
//        int bestIter = -1;
//        for (int i = 0; i < ranks.size(); i++) {
//            for(int j = 0; j<lambdas.size();j++){
//                for(int k=0; k < iters.size();k++){
//                    MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(rdd), ranks.get(i), iters.get(i), lambdas.get(i));
//                    //MatrixFactorizationModel model =ALS.train(rdd,)
//                    //validate the model Rnse
//                   double validateRnse = variance(model,validateData_Rating,validateData_Rating.count());
//                    System.out.println("validation = " + validateRnse + " for the model trained with rank = " + ranks.get(i) + " lambda = " + lambdas.get(i) + " and numIter" + iters.get(i));
//
//                    if (validateRnse < bestValidateRnse) {
//                        bestModel = model;
//                        bestValidateRnse = validateRnse;
//                        bestRank = ranks.get(i);
//                        bestLambda = lambdas.get(i);
//                        bestIter = iters.get(i);
//                    }
//                }
//
//            }
//        }
//        return bestModel;
//    }

    public ALSModel traningALSDataSet(Dataset<Row> traning,Dataset<Row> testdata){
        List<Integer> ranks = new ArrayList<Integer>();
        ranks.add(8);

        List<Double> RegParam = new ArrayList<Double>();
        RegParam.add(0.1);
        RegParam.add(0.5);

        List<Integer> iters = new ArrayList<Integer>();
        iters.add(5);
        iters.add(10);


        ALSModel bestModel = null;
        double bestValidateRnse = Double.MAX_VALUE;
        int bestRank = 0;
        double bestRegParam = -1.0;
        int bestIter = -1;
        for (int i = 0; i < ranks.size(); i++) {
            for (int j = 0; j < RegParam.size(); j++) {
                for (int k = 0; k < iters.size(); k++) {
                    //setting model traning param
                    ALS als = new ALS()
                            .setMaxIter(iters.get(k))
                            .setRegParam(RegParam.get(j))
                            .setRank(ranks.get(i))
                            .setUserCol("userId")
                            .setItemCol("itemId")
                            .setRatingCol("clicktime");
                    ALSModel model = als.fit(traning);
                    //validate best variance return best model
                    double validatevariance = varianceDataset(model, testdata);
                    System.out.println("validation = " + validatevariance + " for the model trained with rank = " + ranks.get(i) + " setRegParam = " + RegParam.get(i) + " and numIter" + iters.get(i));

                    if (validatevariance < bestValidateRnse) {
                        bestModel = model;
                        bestValidateRnse = validatevariance;
                        bestRank = ranks.get(i);
                        bestRegParam = RegParam.get(j);
                        bestIter = iters.get(k);
                    }
                }
            }
        }
        System.out.println("best model is"+"bestRank===="+bestRank+"==bestRegParam is==="+bestRegParam+"===bestIter is="+bestIter);
        return bestModel;
    }

    public static double varianceDataset(ALSModel model,Dataset<Row> testdata){
        model.setColdStartStrategy("drop");
        Dataset<Row> predictions = model.transform(testdata);

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol("clicktime")
                .setPredictionCol("prediction");
        Double rmse = evaluator.evaluate(predictions);
        System.out.println("Root-mean-square error = " + rmse);
        return rmse;
    }

//    /**
//     * @param model
//     * @param predictionData  baseline
//     * @param n
//     * @return
//     */
//    public static double variance(MatrixFactorizationModel model, JavaRDD<Rating> predictionData, long n){
//
//        //change predictionData to tuple2
//        JavaRDD<Tuple2<Object, Object>> userProducts=predictionData.map(new Function <Rating ,Tuple2<Object, Object>>(){
//
//            public Tuple2<Object, Object> call(Rating x) throws Exception {
//                return new Tuple2<Object, Object>(x.user(),x.product());
//            }
//        });
//
//        model.predict(JavaRDD.toRDD(userProducts));
//        /* predict the model */
//        JavaPairRDD<Tuple2<Integer, Integer>, Double> prediction=JavaPairRDD.fromJavaRDD(model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
//                                                                                                                                                        public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating rating) throws Exception {
//                                                                                                                                                            return new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(rating.user(),rating.product()),rating.rating());
//                                                                                                                                                        }
//                                                                                                                                                    }
//
//
//                )
//        );
//
//        //iner join the original data and prediction
//        JavaRDD<Tuple2<Double, Double>> ratesAndPreds=JavaPairRDD.fromJavaRDD(predictionData.map(new Function <Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
//            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating rating) throws Exception {
//                return null;
//            }
//        })).join(prediction).values();
//
//        //compute the rend
//        Double dVar = ratesAndPreds.map(new Function<Tuple2<Double,Double>, Double>() {
//
//                                            public Double call(Tuple2<Double, Double> v1) throws Exception {
//                System.out.print("v1._1 ==========="+v1._1+" and v1._2=========="+v1._2);
//                return (v1._1 - v1._2) * (v1._1 - v1._2);
//            }
//                                        }
//
//
//        ).reduce(new Function2<Double, Double, Double>() {
//            public Double call(Double v1, Double v2) throws Exception {
//                System.out.print("v1==========="+v1+" and v2=========="+v2);
//                return v1+v2;
//            }
//        });
//
//        return Math.sqrt(dVar / n);
//    }

    public Dataset<Row> initalFunction(String address){

        System.setProperty("spark.cores.max", "2");

       // SparkSession spark = SparkSession.builder().config("spark.sql.warehouse.dir","E:/ideaWorkspace/ScalaSparkMl/spark-warehouse").master("local").appName("ALSExample").getOrCreate();
        conf  = new SparkConf().setAppName("ALSModelTraning").setMaster("spark://mopbz5231.mop.fr.ibm.com:7077");
        //conf.set("spark.testing.memory","2147480000");

        sc=new SparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);
        //load the data from hdfs
        System.out.println("+++++++++++Load the text file from hdfs++++++++++++++");
        JavaRDD<Click> ratingsRDD = sc.textFile(address,1).persist(StorageLevel.MEMORY_AND_DISK()).toJavaRDD().map(Click::parseRating);
        Dataset<Row> ratings = sqlContext.createDataFrame(ratingsRDD, Click.class);
        // JavaRDD<String> productdata = JavaSparkContext.fromSparkContext(sc).textFile(address);

        System.out.println("+++++++++++Load completed++++++++++++++");
        return ratings;
    }
}
