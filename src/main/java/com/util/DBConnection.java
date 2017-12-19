package com.util;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Properties;

public class DBConnection implements Serializable {


    public void insert(SparkContext sc, Dataset<Row>dataset){
        System.out.println("ready to write data into DB==============");
        SQLContext sqlContext = new SQLContext(sc);
        Properties properties = new Properties();
        dataset.show();

            System.out.println("using remote DB==============");
            properties.put("user", "myadmin");
            properties.put("password", "12345678");
            String url = "jdbc:mysql://9.110.24.109:3306/fish?useUnicode=true&serverTimezone=UTC";
            //dataset.withColumn("userid","itemid");
            try{
                dataset.write().mode(SaveMode.Append).jdbc(url,"t_recommend",properties);
            }catch (Exception e){
                System.out.println(e);
            }

//        val stud_scoreDF = sqlContext.jdbc(url,"stud_score",properties);
//                    stud_scoreDF.show()

    }
}
