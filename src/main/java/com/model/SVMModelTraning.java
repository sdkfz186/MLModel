package com.model;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

import java.io.Serializable;

 public  class SVMModelTraning  implements Serializable{
    private transient SparkContext sc;
    private JavaSparkContext jsc;
    JavaRDD<LabeledPoint> productdata;

    JavaRDD<LabeledPoint> training;
    JavaRDD<LabeledPoint> test;
    SVMModel model;
    transient SparkConf conf;
    public JavaRDD<LabeledPoint> initalFunction(String address){
        conf  = new SparkConf().setAppName("SVMModelTraning").setMaster("spark://mopbz5231.mop.fr.ibm.com:7077");
        //conf.set("spark.testing.memory","2147480000");

        sc=new SparkContext(conf);
        //load the data from hdfs
       // JavaRDD<String> productdata = JavaSparkContext.fromSparkContext(sc).textFile(address);
       System.out.println("+++++++++++Load the svm file from hdfs++++++++++++++");
        productdata=MLUtils.loadLibSVMFile(sc,address).toJavaRDD();
        System.out.println("+++++++++++Load completed++++++++++++++");
        return productdata;
    }
//TRANING MODEL

    /**
     *
     * @param javardd data from oraginal text
     * @param rate rate for traning and testing
     * @param iteration  number of iteration
     * @return
     */
    public double traningSVM(JavaRDD<LabeledPoint> javardd,double rate,int iteration){
        training = javardd.sample(false, rate, 11L);
        training.cache();
        test = javardd.subtract(training);
        System.out.println("+++++++++++Start traning++++++++++++++");
        model = SVMWithSGD.train(training.rdd(), iteration);
        System.out.println("+++++++++++traning complete++++++++++++++");
        model.clearThreshold();
        model.save(sc, "/model/javaSVMWithSGDModel");

//        double goodvariance=0.0
//        for(int i=0; i<iteration;i++){
//
//        }
        System.out.println("+++++++++++model save complete++++++++++++++");
        double variance=variance();

        return variance;

    }


    private   double variance(){
        JavaRDD<Tuple2<Object, Object>> scoreAndLabels = this.test.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    @Override

                    public Tuple2<Object, Object> call(LabeledPoint p) throws Exception {
                        //double s=model.predict(p.features());
                        return new Tuple2<Object, Object>(model.predict(p.features()),p.label());
                    }
                }
        );

        //user AUC validate the model (0.5-1) is good
        BinaryClassificationMetrics metrics =
                new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
        double auROC = metrics.areaUnderROC();
        return auROC;
    }


    public SVMModel loadmodelfrom(String address){
        SparkConf conf = new SparkConf().setAppName("SVMModelLoad").setMaster("spark://mopbz5231.mop.fr.ibm.com:7077");
        conf.set("spark.testing.memory","2147480000");


        sc=new SparkContext(conf);
        SVMModel sameModel = SVMModel.load(sc, address);
        return sameModel;
    }

}
