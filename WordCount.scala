/**
 * Created by jenny on 10/25/15.
 */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io._

object WordCount {
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val conf = new SparkConf()
      .setAppName("SelectTopUsers")
      .set("spark.executor.memory", "6g")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val base = "../Downloads/RadiusDataEngineeringExercise/"
    val transRDD = sc.textFile(base + "transactions.txt")
    val donotcallRDD = sc.textFile(base + "donotcall.txt")
    val donotcallPairRDD = donotcallRDD.map(x => (x, 1))
    val usersRDD = sc.textFile(base + "users.txt")

    case class infoAmount(id:String, name: String, phone: String, amount:Double)

    /*
     * Parse transaction records
     */
    val YEAR = "2015"
    val mergedTransRDD = transRDD
           .map(_.replace("$", ""))  // remove $ for arranging amounts in desc order
           .map(_.replace("-", ";")) //replace 2015-09-05 with 2015;09;05 to split and remove month/date
           .map(_.split(";"))  // convert to Array(815581247, 144.82, 2015, 09, 05)
           .filter{ x => x(2) == YEAR} // keep 2015 entries
           .map{arr => (arr(0), (arr(1).toDouble))} // keep customerID and amount
           .reduceByKey(_ + _)  //merge multiple purchases from the same customer
           .cache()

    println("Number of transaction records " + mergedTransRDD.count)
    println("Typical merged transaction record:" + mergedTransRDD.first)

    /*
     * Parse user records and inner join with transaction RDD
     */
    val UsersAmountRDD = usersRDD
           .filter(hasPhoneNum(_)) //if there is no phone number, remove the entry
           .map(_.split(";"))
           .map(_.filter(!isEmail(_))) //for each entry, remove email
           .map{case items => ((items(0), items(1)), items(2))}  //arrange in key value pair ((id, name), phone)
           .flatMapValues(x => x.split(",")) //if a customer has multiple phone numbers, generate separate entries
           .map{case (cust, phone) => (phone, cust)}
           .subtractByKey(donotcallPairRDD)  //remove phone numbers in donot call list
           .filter{case (phone, cust) => (phone != "")}
           .map{case (phone, cust) => (cust, phone)}
           .reduceByKey(_ + "," + _)  //merge phone numbers from the same customer
           .map{case (cust, phone) => (cust._1, (cust._2, phone))}
           .join(mergedTransRDD)
           .map{case (id, (info, amount)) => infoAmount(id, info._1, info._2, amount)}
           .cache()

    println("Number of user transaction records " + UsersAmountRDD.count)
    println("Typical user transaction record:" + UsersAmountRDD.first)

    /*
     * Select top 1000 users and write to a text file
     */
    val SEP = ";"
    val NUM_OF_USER = 1000
    val topUsers = UsersAmountRDD
      .takeOrdered(NUM_OF_USER)(Ordering[Double].reverse.on(x=>x.amount))
      .map(x => x.id + SEP + x.name + SEP + x.phone + SEP + "$" + "%.2f".format(x.amount))

    val pw = new PrintWriter(new File("/Users/jenny/Downloads/top_users.txt" ))
    for (elem <- topUsers) {
      pw.write(elem)
      pw.write("\n")
    }
    pw.close

    sc.stop()
    val elapsed = System.currentTimeMillis() - start
    println ("elapsed time in milliseconds " + elapsed)
  }

  def isEmail(item: String): Boolean = {
    item.contains("@")
  }

  def hasPhoneNum(item: String): Boolean = {
    item.contains("-") && item.contains("(")
  }
}