package com.control;

import com.abstractclass.JavaForeachPartitionFunc;
import com.util.DBConnection;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Function1;
import scala.collection.Iterable;
import scala.collection.Iterator;
import scala.collection.mutable.Seq;


import java.io.Serializable;
import java.sql.*;

import java.util.ArrayList;
import java.util.List;

public class Recommend implements Serializable {
    private SparkContext sc;
    private JavaSparkContext jsc;
    private ALSModel model=null;

    String pructdetailadress = "";

    public ALSModel initalmodel(){
        SparkConf conf = new SparkConf().setAppName("recommend").setMaster("spark://mopbz5231.mop.fr.ibm.com:7077");
        //jsc= new JavaSparkContext(conf);
        sc=new SparkContext(conf);
        //JavaSparkContext.fromSparkContext(sc)
        model=loadModelFromHDFS(sc,"/model/recomand/model");
        System.out.println("Load model completed");
        return model;
    }


    public ALSModel  loadModelFromHDFS(SparkContext sc,String address){

        return ALSModel.load(address);
    }

    public void updateRecomandTable(Dataset<Row> dataset) {
        Dataset<Row> newdataset = dataset.toDF("userid", "itemid");
        //DataFrameReader newdataset=dataset.toDF("userid", "itemid");

        newdataset.printSchema();
        newdataset.show();
        System.out.println(" already get recommendation  paparing to update database");

        String sql = "REPLACE INTO fish.t_recommend (userid,itemid) VALUES(?, ?)";
        try {

            //DBConnection dbConnection=new DBConnection();
           // dbConnection.insert(sc,newdataset);

            //connection = DriverManager.getConnection("jdbc:mysql://9.110.24.109:3306/fish?useUnicode=true&serverTimezone=UTC", "myadmin", "12345678");

            System.out.println(" ready to insert....");
//            newdataset.foreachPartition(it -> {
//                while (it.hasNext()){
//                    Connection connection = null;
//                    //connection = DriverManager.getConnection("jdbc:mysql://9.112.52.88:3306/fish?useUnicode=true&serverTimezone=UTC", "root", "201702");
//                    connection = DriverManager.getConnection("jdbc:mysql://9.110.24.109:3306/fish?useUnicode=true&serverTimezone=UTC", "myadmin", "12345678");
//                    PreparedStatement ps = connection.prepareStatement(sql);
//                    Row row=(Row)it.next();
//                    int userid=row.getAs("userid");
//                    //String itemid=row.getList(1).toString();
//                    //System.out.println("out put row infor userid=========================="+userid);
//
//                    ps.setInt(1, userid);
//                    ps.setString(2, row.getList(1).toString());
//
//                    //ps.setDate(3, new java.sql.Date(new java.util.Date().getTime()));
//                    ps.execute();
//
//                    //List li=row.getList(1);
//                    //System.out.println("out put row infor  li====================================="+li);
//
//                }
//            });
            //ps.executeBatch();

            Connection connection = null;
            //connection = DriverManager.getConnection("jdbc:mysql://9.112.52.88:3306/fish?useUnicode=true&serverTimezone=UTC", "root", "201702");
            connection = DriverManager.getConnection("jdbc:mysql://9.110.24.109:3306/fish?useUnicode=true&serverTimezone=UTC", "myadmin", "12345678");
            PreparedStatement ps = connection.prepareStatement(sql);
            List<Row>li=newdataset.collectAsList();
            System.out.println("collectAsList successful");
            for (Row row:li) {

                int userid=row.getAs("userid");
                System.out.println("userid ==============="+userid);
                ps.setInt(1, userid);
                ps.setString(2, row.getList(1).toString());
                ps.addBatch();
            }
            ps.executeBatch();

        } catch (Exception e) {

        }

//        newdataset.map(new MapFunction<Row, Row>() {
//
//            @Override
//            public Row call(Row value) throws Exception {
//                int userid=value.getAs("userid");
//                String itemlist=value.getList(1).toString();
//
//            }
//        });
    }



//    public  String recmmendProduct(int i,int number) {
//        initalmodel();
//        String s= String.valueOf(model.recommendProducts(i,number));
//        return s;
//    }

//    public JavaRDD<Tuple2<Integer, String>> LoadProductDetail(String adress){
//        JavaRDD<String> productdata = JavaSparkContext.fromSparkContext(sc).textFile(adress);
//
//        //convert rdd to tuple3
//        JavaRDD<Tuple3<Integer, String, String>> productList_Tuple=productdata.map(new Function<String, Tuple3<Integer, String, String>>() {
//            public Tuple3<Integer, String, String> call(String sline) throws Exception {
//                String[] fields = sline.split("::");
//                if (fields.length != 3) {
//                    throw new IllegalArgumentException("Each line must contain 3 fields");
//                }
//                int id = Integer.parseInt(fields[0]);
//                String title = fields[1];
//                String type = fields[2];
//
//                return new Tuple3<Integer, String, String>(id,title,type);
//            }
//        });
//
//        //convert tuple3 to tuple2
//        JavaRDD<Tuple2<Integer, String>> product_Map=productList_Tuple.map(new Function<Tuple3<Integer, String, String>, Tuple2<Integer, String>>() {
//            public Tuple2<Integer, String> call(Tuple3<Integer, String, String> v) throws Exception {
//                return new Tuple2<Integer, String>(v._1(),v._2());
//            }
//        });
//        System.out.println("product recommond for you:");
//        return product_Map;
//    }

}
