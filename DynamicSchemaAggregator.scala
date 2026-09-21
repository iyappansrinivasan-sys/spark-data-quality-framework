import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DynamicSchemaAggregator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Dynamic Schema-Aware Aggregations & Profiling")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // 1. Create a sample dataset with mixed data types
    val sampleDF = Seq(
      ("2026-01-01", "Alice", 150.75, 10),
      ("2026-01-02", "Bob", 300.50, 20)
    ).toDF("tx_date", "customer_name", "amount", "quantity")

    // Cast columns to their correct native Spark types for demonstration
    val typedDF = sampleDF
      .withColumn("tx_date", to_date($"tx_date"))
      .withColumn("amount", $"amount".cast(DoubleType))
      .withColumn("quantity", $"quantity".cast(IntegerType))

    println("--- Original Dataset Schema ---")
    typedDF.printSchema()

    // 2. Generate and apply dynamic aggregations using the schema-inspection function
    val aggExprs = buildDynamicAggs(typedDF)

    println("--- Dynamic Aggregation Profiling Results ---")
    typedDF.agg(aggExprs.head, aggExprs.tail:_*).show(false)

    spark.stop()
  }

  /**
   * Function that inspects DataFrame schema fields and builds 
   * smart type-specific aggregation expressions dynamically.
   */
  def buildDynamicAggs(df: DataFrame): Seq[Column] = {
    df.schema.fields.map { field =>
      val c = col(field.name)
      field.dataType match {
        // 1. Numeric types: compute sum
        case _: IntegerType | _: LongType | _: ShortType | _: ByteType | _: DoubleType | _: FloatType | _: DecimalType =>
          sum(c).as(s"sum_${field.name}")

        // 2. String types: sum of character lengths
        case _: StringType =>
          sum(length(c)).as(s"sum_len_${field.name}")

        // 3. Date / Timestamp types: lowest and highest date boundaries
        case _: DateType | _: TimestampType =>
          struct(min(c).as("min"), max(c).as("max")).as(s"min_max_${field.name}")

        // Fallback for other complex or unexpected types
        case _ =>
          count(c).as(s"cnt_${field.name}")
      }
    }
  }
}
