package com.control;


import com.model.SVMModelTraning;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.ArrayList;

public class SVMPredict implements Serializable {
    SVMModelTraning svmmodel;
    JavaRDD<LabeledPoint> productdata;
    SVMModel model;
    SVMModel sameModel;
    public SVMPredict(){
        svmmodel=new SVMModelTraning();
    }
    public double traningModel(String address){
        double v=0.0;
        productdata=svmmodel.initalFunction(address);
        v=svmmodel.traningSVM(productdata,0.7,10);
        System.out.println("Model traning completed, The model variance is " +
                "----------------------------------------------------" +
                "=="+v);
        return v;
    }

    /**
     * load the model from hdfs be trained
     * @return SVMModel
     */
    public SVMModel loadmodel(String address){
        return svmmodel.loadmodelfrom(address);
    }

    public void predict(SVMModel model,int[] index,double[] point){
        Vector dv = Vectors.sparse(60, index,point);

        double type=model.predict(dv);
        System.out.println("the model predict result is=========="+type);
    }
}
