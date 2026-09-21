import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DataQualityReconciliation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Enterprise Data Quality & Reconciliation")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // ==========================================
    // 1. SIMULATING HIGH-CARDINALITY SALTING
    // ==========================================
    println("--- 1. Running Salting Mechanism for Skewed Joins ---")
    
    val transactions = Seq(
      ("T1", "M_POPULAR", 100.0),
      ("T2", "M_POPULAR", 250.0),
      ("T3", "M_RARE", 50.0)
    ).toDF("txn_id", "merchant_id", "amount")

    val merchants = Seq(
      ("M_POPULAR", "Global Mega Vendor"),
      ("M_RARE", "Local Boutique")
    ).toDF("merchant_id", "merchant_name")

    // Add a random salt suffix (e.g., _0, _1, _2) to break up the hot merchant key
    val numSalts = 3
    val saltedTransactions = transactions
      .withColumn("salted_merchant_id", concat($"merchant_id", lit("_"), (rand() * numSalts).cast("int")))

    // Expand the dimension table so every merchant matches all possible salt buckets
    val saltSeq = lit(numSalts)
    val expandedMerchants = merchants
      .withColumn("salt_array", array((0 until numSalts).map(lit): _*))
      .withColumn("single_salt", explode($"salt_array"))
      .withColumn("salted_merchant_id", concat($"merchant_id", lit("_"), $"single_salt"))
      .drop("salt_array", "single_salt")

    // Perform the join safely on the salted key, then drop helper columns
    val joinedTransactions = saltedTransactions
      .join(expandedMerchants, Seq("salted_merchant_id"), "inner")
      .drop("salted_merchant_id", "merchant_id")

    joinedTransactions.show(false)

    // ==========================================
    // 2. SOURCE-TO-TARGET RECONCILIATION
    // ==========================================
    println("--- 2. Running Source-to-Target Audit (Full Outer Join) ---")

    // Simulate Source (Oracle) vs Target (Databricks Delta Table)
    val sourceDF = Seq(
      ("101", "Alice", 500.0),
      ("102", "Bob", 300.0), // Missing in Target (Data Loss)
      ("103", "Charlie", 450.0)
    ).toDF("id", "name", "amount")

    val targetDF = Seq(
      ("101", "Alice", 500.0),
      ("103", "Charlie", 400.0) // Value Mismatch on amount!
    ).toDF("id", "name", "amount")

    val src = sourceDF.as("src")
    val tgt = targetDF.as("tgt")

    // Use Full Outer Join to catch everything without dropping missing records
    val reconciliationDF = src.join(tgt, Seq("id"), "full")
      .withColumn("qc_status",
        when($"src.id".isNull, lit("Orphan / Extra in Target"))
        .when($"tgt.id".isNull, lit("Missing in Target (Data Loss!)"))
        .when($"src.name" =!= $"tgt.name" || $"src.amount" =!= $"tgt.amount", lit("Value Mismatch"))
        .otherwise(lit("Match"))
      )

    reconciliationDF.select("id", "src.name", "src.amount", "tgt.amount", "qc_status").show(false)

    // ==========================================
    // 3. DETECTING DELETIONS VIA ANTI-JOIN
    // ==========================================
    println("--- 3. Running Left Anti-Join for Upstream Deletions ---")
    
    // Finds records present in Source but completely vanished from Target
    val deletionsDF = sourceDF.join(targetDF, Seq("id"), "left_anti")
    
    deletionsDF.show(false)
    
    spark.stop()
  }
}
