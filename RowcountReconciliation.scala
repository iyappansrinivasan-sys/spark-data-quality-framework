import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RowcountReconciliation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Source to Target Row Count Reconciliation")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // ==========================================================
    // 1. SIMULATE LOADING SOURCE (Oracle) AND DESTINATION DATA
    // ==========================================================
    
    // Simulate reading records from the source application / table
    val sourceDF = Seq(
      ("T1", "2026-09-20"),
      ("T2", "2026-09-20"),
      ("T3", "2026-09-20")
    ).toDF("id", "txn_date")

    // Simulate reading records from the destination Databricks table
    val targetDF = Seq(
      ("T1", "2026-09-20"),
      ("T2", "2026-09-20"),
      ("T3", "2026-09-20")
    ).toDF("id", "txn_date")

    // ==========================================================
    // 2. DYNAMICALLY COUNT THE NUMBER OF RECORDS
    // ==========================================================
    
    val sourceCount: Long = sourceDF.count()
    val targetCount: Long = targetDF.count()

    // ==========================================================
    // 3. CALCULATE VARIANCE & ENFORCE 0.0% LENIENCY
    // ==========================================================
    
    val absoluteDifference = Math.abs(sourceCount - targetCount)
    
    // Calculate percentage variance: (Difference / Source Count) * 100
    val variancePercentage = if (sourceCount > 0) {
      (absoluteDifference.toDouble / sourceCount.toDouble) * 100.0
    } else {
      0.0
    }

    println("==================================================")
    println(s"SOURCE TABLE RECORD COUNT:      $sourceCount")
    println(s"DESTINATION TABLE RECORD COUNT: $targetCount")
    println(s"ABSOLUTE DIFFERENCE:            $absoluteDifference")
    println(f"VARIANCE PERCENTAGE:            $variancePercentage%.4f%%")
    println("==================================================")

    // Strict 0.0% Leniency Validation Check
    val allowedLeniencyPercentage = 0.0

    if (variancePercentage <= allowedLeniencyPercentage) {
      println("✅ QC PASS: Row count reconciliation successful! Exact match achieved with 0.0% leniency.")
    } else {
      throw new RuntimeException(
        s"❌ QC FAIL: Row count mismatch detected! Variance of $variancePercentage% violates the strict 0.0% threshold."
      )
    }

    spark.stop()
  }
}
