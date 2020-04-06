import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.task.core.agg.SessionAggregator
import com.task.core.infrastructure.{DataLoad, Spark}
import com.task.core.jobs.MarketingAnalysisJobProcessor
import com.task.core.models.Event
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalactic.Explicitly._
import org.scalatest.Matchers._
import org.apache.spark.sql.functions.rand

class TaskTests extends AnyFlatSpec
  with DatasetSuiteBase with DataLoad {

  case class Purchase(purchaseId: String, purchaseTime: String,
                      billingCost: String, isConfirmed: Boolean,
                      sessionId: String, campaignId: String, channelIid: String)

  case class Campaigns(MarketingCampaign: String, Revenue: Double)

  case class Engagements(Campaign: String, TopChannel: String)

  implicit def sparkSession: SparkSession = spark

  import spark.implicits._

  var events: DataFrame = _
  var purchases: DataFrame = _
  var processor: MarketingAnalysisJobProcessor = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val data = loadData
    events = data._1
      .orderBy(rand())//correctness should not depends on order
    purchases = data._2
      .orderBy(rand())//correctness should not depends on order
    processor = new MarketingAnalysisJobProcessor(events, purchases)
    org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
  }

  behavior of "etl"

  it should "basic tests" in {
    val res = processor.saveAndGetPurchases
      .as[Purchase]
      .collect()

    assert(res nonEmpty, "should contain rows")
    assert(res.count(_.isConfirmed) == 4, "4 confirmed purchases")
    assert(res.count(!_.isConfirmed) == 2, "2 not confirmed purchases")
    assert(res.filter(_.purchaseId == "p4").head.sessionId == res.filter(_.purchaseId == "p5").head.sessionId, "should be done in one session")
  }

  it should "basic tests for aggregator" in {
    val sessions = events
      .as[Event]
      .groupByKey(r => r.userId)
      .agg(SessionAggregator.toColumn)
      .flatMap(_._2)
      .collect()

    assert(sessions.groupBy(_.sessionId).keys.toList.length == 7, "should be 7 sessions")
  }

  it should "advanced tests for aggregator" in {
    val res = processor.purchasesViaAggregator
      .as[Purchase]
      .collect()

    assert(res.groupBy(_.sessionId).keys.toList.length == 5, "should be 5 purchase sessions")
    assert(res nonEmpty, "should contain rows")
    assert(res.count(_.isConfirmed) == 4, "4 confirmed purchases")
    assert(res.count(!_.isConfirmed) == 2, "2 not confirmed purchases")
    assert(res.filter(_.purchaseId == "p4").head.sessionId == res.filter(_.purchaseId == "p5").head.sessionId, "should be done in one session")
  }

  it should "for top campaigns" in {
    val res = processor.topCampaigns(10)
      .as[Campaigns]
      .collect()

    assert(res nonEmpty, "should contain rows")
    assert(res.head == Campaigns("cmp1", 300.5), "should be cmp1 300.5")
    assert(res.length == 2, "contains only 2 campaigns")
  }

  it should "for top channels" in {
    val res = processor.channelsEngagementPerformance
      .as[Engagements]
      .collect()

    assert(res nonEmpty, "should contain rows")

    res should contain(Engagements("cmp1", "Google Ads"))
    res should contain(Engagements("cmp2", "Yandex Ads"))
  }
}