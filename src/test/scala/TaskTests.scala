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
}
