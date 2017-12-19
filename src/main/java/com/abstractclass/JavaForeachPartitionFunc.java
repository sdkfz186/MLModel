package com.abstractclass;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.Serializable;
import scala.collection.Iterator;

public abstract class JavaForeachPartitionFunc extends AbstractFunction1<Iterator<Row>, BoxedUnit> implements Serializable {
    public BoxedUnit apply(Iterator<Row> it) {
        call(it);
        return BoxedUnit.UNIT;
    }

    public abstract void call(Iterator<Row> it);
}
