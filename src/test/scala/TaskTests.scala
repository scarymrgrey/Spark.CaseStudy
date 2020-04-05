import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.task.core.jobs.MarketingAnalysisJobProcessor
import com.task.infastructure.{AppSettings, DataLoader, WithSettings}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.Matchers._
import org.scalatest.flatspec.AnyFlatSpec

class TaskTests extends AnyFlatSpec
  with DatasetSuiteBase with DataLoader with WithSettings {

  case class Purchase(purchaseId: String, purchaseTime: String,
                      billingCost: String, isConfirmed: Boolean,
                      sessionId: String, campaignId: String, channelIid: String)

  case class Campaigns(campaignId: String)

  case class Engagements(Campaign: String, TopChannel: String)

  implicit def sparkSession: SparkSession = spark

  import spark.implicits._

  var sessions: DataFrame = _
  var purchases: DataFrame = _
  var processor: MarketingAnalysisJobProcessor = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val data = loadFromSettingsWithArgs(Array())
    sessions = data._1
    purchases = data._2
    processor = new MarketingAnalysisJobProcessor(sessions, purchases)
    org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
  }

  behavior of "etl"

  it should "basic tests" in {
    val res = processor.getPurchasesWithSessions
      ._2
      .as[Purchase]
      .collect()

    assert(res nonEmpty, "should contain rows")
    assert(res.count(_.isConfirmed) == 4, "4 confirmed purchases")
    assert(res.count(!_.isConfirmed) == 2, "2 not confirmed purchases")
  }

  it should "basic tests for aggregator" in {
    val res = processor.purchasesViaAggregator
      .as[Purchase]
      .collect()

    assert(res nonEmpty, "should contain rows")
    assert(res.count(_.isConfirmed) == 4, "4 confirmed purchases")
  }

  it should "for top campaigns" in {
    val (_,aggPurchases) = processor.getPurchasesWithSessions

    val res = processor.topCompaigns(10, aggPurchases)
      .as[Campaigns]
      .collect()

    assert(res nonEmpty, "should contain rows")
    assert(res.head == Campaigns("cmp1"), "should be")
    assert(res.length == 2, "contains only 2 campaigns")
  }

  it should "for top channels" in {
    val res = processor.channelsEngagementPerformance(sessions)
      .as[Engagements]
      .collect()

    assert(res nonEmpty, "should contain rows")

    res should contain(Engagements("cmp1", "Google Ads"))
    res should contain(Engagements("cmp2", "Yandex Ads"))
  }
}
