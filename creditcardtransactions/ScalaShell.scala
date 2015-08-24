// val mergedFile = "A_merged/"
val file = "B_someManualCleaning/"
val lines = sc.textFile(file, 32)
import util.matching.Regex.Match
def removeComma(m: Match) = {
  m.matched.replace(",", "")
}
val amountRegEx = """\d,\d\d\d.\d\d""".r
val records = lines.map(l => amountRegEx.replaceAllIn(l, removeComma(_))).filter(l => !l.toUpperCase.startsWith("ACCOUNT") && !l.trim.isEmpty)
//val badLines = lines.filter(l => l.toUpperCase.startsWith("ACCOUNT") || l.trim.isEmpty)
//badLines.count
//badLines take 3 // show 3 examples
//badLines.collect foreach println
val filteredOutputDirectory = "C_cleaned/"
records.saveAsTextFile(filteredOutputDirectory)

import java.util.List
import collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable,Text}
import org.apache.hadoop.mapreduce.lib.input.{CSVLineRecordReader,CSVTextInputFormat}
val conf = new Configuration()
conf.set(CSVLineRecordReader.FORMAT_DELIMITER, CSVLineRecordReader.DEFAULT_DELIMITER)
conf.set(CSVLineRecordReader.FORMAT_SEPARATOR, CSVLineRecordReader.DEFAULT_SEPARATOR)
conf.setBoolean(CSVLineRecordReader.IS_ZIPFILE, false)

val hadoopFile = sc.newAPIHadoopFile[LongWritable,List[Text],CSVTextInputFormat](filteredOutputDirectory, classOf[CSVTextInputFormat], classOf[LongWritable], classOf[List[Text]], conf)
val columns = hadoopFile.map(_._2.asScala.toList.take(27).map(_.toString))//.cache
columns take 1
//val dirtyRecords = columns.filter(_.length != 27)//.cache // data quality check for correct number of columns
//dirtyRecords.filter(_.size > 1).count // only 2 lines that CSVTextInputFormat apparently didn't handle correctly due to an odd character // TODO find and filter out that weird character
val cleanRecords = columns.filter(_.length == 27)
import org.apache.spark.sql.Row
val transactionRows = cleanRecords.map(c => Row(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10), c(11), c(12), c(13), c(14), c(15), c(16), c(17), c(18), c(19), c(20), c(21), c(22), c(23), c(24), c(25), c(26)))
import sqlContext.implicits._
import org.apache.spark.sql.types._
val schema = StructType(
  StructField("account", StringType) ::
    StructField("account_name", StringType) ::
    StructField("amount", StringType) ::
    StructField("auth_code", StringType) ::
    StructField("avs", StringType) ::
    StructField("brand", StringType) ::
    StructField("card_ending", StringType) ::
    StructField("cvd", StringType) ::
    StructField("unknown", StringType) ::
    StructField("name", StringType) ::
    StructField("merchant_trans_id", StringType) ::
    StructField("option_code", StringType) ::
    StructField("dateandtime", StringType) ::
    StructField("txn_id", StringType) ::
    StructField("conf_no", StringType) ::
    StructField("error_code", StringType) ::
    StructField("auth_type", StringType) ::
    StructField("type", StringType) ::
    StructField("city", StringType) ::
    StructField("country", StringType) ::
    StructField("email", StringType) ::
    StructField("phone", StringType) ::
    StructField("state", StringType) ::
    StructField("address1", StringType) ::
    StructField("address2", StringType) ::
    StructField("zip", StringType) ::
    StructField("ip_address", StringType) ::
    Nil
)
val transactionsDataFrame = sqlContext.createDataFrame(transactionRows, schema)
val finalOutputTableDirectory = "D_json/"
import org.apache.spark.sql.SaveMode
transactionsDataFrame.write.format("json").mode(SaveMode.Overwrite).save(finalOutputTableDirectory) // Note: Saving as Parquet requires tons of memory
transactionsDataFrame.count // 9685914
// data quality checks for commas within text fields
transactionsDataFrame.registerTempTable("transactions")
def possiblyMalformedRows(column: String) = sqlContext.sql(s"SELECT $column FROM transactions WHERE $column LIKE '%\n%'").collect
