package ai.onehouse.transformers;

import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class Timestamper implements Transformer {
  private static final Logger log = LogManager.getLogger(Timestamper.class);

  public Dataset<Row> apply(JavaSparkContext javaSparkContext, SparkSession sparkSession, Dataset<Row> dataset,
      TypedProperties typedProperties) {
    String timestampColumn = typedProperties.getString("timestamp.column", "_timestamp");
    String timestampGranularity = typedProperties.getString("timestamp.granularity", "millisecond");

    log.info("Within timestamp transformer, field: " + timestampColumn
        + " granularity: " + timestampGranularity);

    Set<String> validGranularities = new HashSet<>(Arrays.asList(
        "millisecond", "second", "minute", "hour", "day", "week", "month", "quarter", "year"));

    // force millisecond granularity if invalid
    if (!validGranularities.contains(timestampGranularity)) {
      timestampGranularity = "millisecond";
    }

    // return dataset.withColumn(timestampColumn,
    // functions.date_trunc(timestampGranularity,
    // functions.current_timestamp()).cast(DataTypes.TimestampType));

    // debugging with the simplest case
    return dataset.withColumn(timestampColumn, functions.current_timestamp());

  }

}
