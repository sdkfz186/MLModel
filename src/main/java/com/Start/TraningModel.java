package com.Start;


import com.control.Recommend;


import com.model.ALSModelTraning;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.Scanner;
import java.util.function.ToIntFunction;

import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Function1;
import scala.Tuple2;


import scala.collection.Iterator;
import scala.collection.Seq;

public class TraningModel implements Serializable {

    private transient SparkContext sc;
    private JavaSparkContext jsc;
    transient SparkConf conf;

    JavaRDD<LabeledPoint> productdata;

    JavaRDD<LabeledPoint> training;
    JavaRDD<LabeledPoint> test;
    public static void main(String[] args) throws Exception{
        String modeluse=args[0];
        if(modeluse.equals("0")){
            ALSModelTraning alsModelTraning =new ALSModelTraning();
            Dataset<Row> dataset=alsModelTraning.initalFunction("/data/input/Rating_T.dat");

            Dataset<Row>[] splits = dataset.randomSplit(new double[]{0.8, 0.2});
            Dataset<Row> training = splits[0].cache();
            Dataset<Row> test = splits[1].cache();
            System.out.println("begin traning");
            ALSModel model=alsModelTraning.traningALSDataSet(training,test);
            model.write().overwrite().save("/model/recomand/model");
            System.out.println("traning completed");
        }else{
            Recommend recommend =new Recommend();
            ALSModel model=recommend.initalmodel();
            //Sugest 5 item for each user
            Dataset<Row> dataset=model.recommendForAllUsers(5);
            recommend.updateRecomandTable(dataset);
        }

    }


}
