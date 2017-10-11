package com.databricks.spark.sql.perf

import org.apache.commons.io.IOUtils
import com.databricks.spark.sql.perf.tpcds.TPCDSTables

object QueryAnalyzer {

  private val queryNames = Seq(
    "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10",
    "q11", "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19",
    "q20", "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27",
    "q28", "q29", "q30", "q31", "q32", "q33", "q34", "q35", "q36", "q37",
    "q38", "q39a", "q39b", "q40", "q41", "q42", "q43", "q44", "q45", "q46", "q47",
    "q48", "q49", "q50", "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58",
    "q59", "q60", "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68", "q69",
    "q70", "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79",
    "q80", "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89",
    "q90", "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99",
    "ss_max"
  )

  val tpcds2_4Queries = queryNames.map { queryName =>
    val queryContent: String = IOUtils.toString(
      getClass().getClassLoader().getResourceAsStream(s"tpcds_2_4/$queryName.sql"))

    (queryName, queryContent)
  }

  def main(args: Array[String]): Unit = {

    // Get words from queries in the form (key, queryId, count)
    val words = tpcds2_4Queries.flatMap( query => {
      val content = query._2
      content
        .split("\\s+|,|\\(|\\)|`|\\.|\\+|=")
        .map(str => (str.toLowerCase, query._1, 1))
    })

    // Get column keys (assuming keys contains _ charactor)
    val columnKeyPrefix = List(
      "cs_", "cr_", "inv_", "ss_", "sr_", "ws_", "wr_", "cc_" +
      "cp_", "c_", "ca_", "cd_", "d_", "hd_", "ib_", "i_" +
        "p_", "r_", "sm_", "s_", "t_", "w_", "wp_", "web_")
//    val columnKeys = words.filter(k => k._1.contains("_"))
    val columnKeys = words.filter(k => columnKeyPrefix.exists(k._1.startsWith(_)))
    val keyFrequency = columnKeys.groupBy(_._1).mapValues(_.map(_._3).sum).toList

    // Get column keys that are time-based (with top elements manually filtered)
    val timeBasedKeyIds = List("d_year", "d_date_sk", "d_moy",
      "ss_sold_date_sk", "cs_sold_date_sk", "d_date",
      "ws_sold_date_sk", "d_week_seq", "d_month_seq",
      "cs_sold_date_sk", "sr_returned_date_sk")

    val timeBasedKeys = keyFrequency.filter(k => timeBasedKeyIds.contains(k._1))
    val nonTimeBasedkeys = keyFrequency.filter(k => !timeBasedKeyIds.contains(k._1))

    val totalFrequency = keyFrequency.map(_._2).reduce( (a, b) => a + b)
    val timeBasedFrequency = timeBasedKeys.map(_._2).reduce( (a, b) => a + b)
    val nonTimeBasedFrequency = nonTimeBasedkeys.map(_._2).reduce( (a, b) => a + b)

    // Show total key frequency, and those of time-based and non time-based keys.
    println(s"time-based >= $timeBasedFrequency, non time-based <= $nonTimeBasedFrequency, total == $totalFrequency")
    println()
    println("time-based columns: ")
    timeBasedKeys.sortBy(_._2).reverse.take(50).foreach(println(_))
    println()
    println("non-time-based columns: ")
    nonTimeBasedkeys.sortBy( x=> (x._1.substring(0, 2), x._2)).reverse.take(1000).foreach(println(_))


  }
}
