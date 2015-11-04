import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by loaner on 10/26/15.
 */
object AuctionsScalaApp {

  def main(args:Array[String]) {
    val sparkConf = new SparkConf().setAppName("AuctionsScalaApp");
    val sc = new SparkContext(sparkConf);

    val usrhome = System.getenv("HOME")
    val aucFile = usrhome.concat("/Downloads/DEV360Data/auctiondata.csv")

    val auctionid = 0
    val bid = 1
    val bidtime = 2
    val bidder = 3
    val bidderrate = 4
    val openbid = 5
    val price = 6
    val itemtype = 7
    val daystolive = 8

    val auctionsRDD = sc.textFile(aucFile).map(_.split(",")).cache()

    //total number of bids across all auctions
    val totalbids = auctionsRDD.count()

    //total number of items (auctions)
    val totalNumberOfItems = auctionsRDD.map(x=>x(auctionid)).distinct().count()

    //RDD containing ordered pairs of auctionid,number
    val biditems = auctionsRDD.map(x=>(x(auctionid),1)).reduceByKey((x,y)=>x+y)

    //max, min and avg number of bids
    val maxbids= biditems.map(x=>x._2).reduce((x,y)=>Math.max(x,y))
    val minbids= biditems.map(x=>x._2).reduce((x,y)=>Math.min(x,y))
    val avgbids= totalbids / totalNumberOfItems

    //print to console
    println("total bids across all auctions: %s ".format(totalbids))
    println("total number of distinct auctions: %s ".format(totalNumberOfItems))
    println("Max bids across all auctions: %s ".format(maxbids))
    println("Min bids across all auctions: %s ".format(minbids))
    println("Avg bids across all auctions: %s ".format(avgbids))

  }
}
