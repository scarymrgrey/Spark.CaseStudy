import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.task.core.data.DataLoader
import com.task.core.jobs.MarketingAnalysisJobProcessor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FunSuite

class TaskTests extends FunSuite
  with DatasetSuiteBase with DataLoader {

  case class Purchase(purchaseId: String, purchaseTime: String,
                      billingCost: String, isConfirmed: Boolean,
                      sessionId: String, campaignId: String, channelIid: String)

  case class Campaigns(MarketingCampaign: String, Revenue: Double)

  implicit def sparkSession: SparkSession = spark

  import spark.implicits._

  var events: DataFrame = _
  var purchases: DataFrame = _
  var processor: MarketingAnalysisJobProcessor = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val data = loadData()
    events = data._1
    purchases = data._2
    processor = new MarketingAnalysisJobProcessor(events, purchases)
    org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
  }

  test("basic tests") {
    val res = processor.saveAndGetPurchases
      .as[Purchase]
      .collect()

    assert(res nonEmpty, "should contain rows")
    assert(res.count(_.isConfirmed) == 4, "4 confirmed purchases")
  }

  test("basic tests for aggregator") {
    val res = processor.purchasesViaAggregator
      .as[Purchase]
      .collect()

    assert(res nonEmpty, "should contain rows")
    assert(res.count(_.isConfirmed) == 4, "4 confirmed purchases")
  }

  test("for top campaigns") {
    val res = processor.topCampaigns(10)
      .as[Campaigns]
      .collect()

    assert(res nonEmpty, "should contain rows")
    assert(res.head == Campaigns("cmp1",300.5), "should be cmp1 300.5")
    assert(res.length == 2, "contains only 2 campaigns")
  }
}
