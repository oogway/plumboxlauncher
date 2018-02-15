package com.kpit.warehouse.transformer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Transformer {
    Dataset<Row> run(Dataset<Row> events);
}
