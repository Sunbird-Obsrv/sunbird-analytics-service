package org.ekstep.analytics.api.util

import java.util.UUID

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.ekstep.analytics.api.{DateRange, ExperimentBodyResponse, ExperimentParams, Params, Range, Response, ResponseCode}
import org.ekstep.analytics.framework.conf.AppConf
import org.joda.time._
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
 * @author Santhosh
 */
object CommonUtil {

  @transient val dayPeriod: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
  @transient val weekPeriod: DateTimeFormatter = DateTimeFormat.forPattern("yyyy'7'ww").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
  @transient val monthPeriod: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMM").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
  @transient val weekPeriodLabel: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-ww").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
  @transient val df: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ").withZoneUTC();
  @transient val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
  val offset: Long = DateTimeZone.forID("Asia/Kolkata").getOffset(DateTime.now())

  def roundDouble(value: Double, precision: Int): Double = {
    BigDecimal(value).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble;
  }

  def getWeeksBetween(fromDate: Long, toDate: Long): Int = {
    val from = new LocalDate(fromDate, DateTimeZone.UTC)
    val to = new LocalDate(toDate, DateTimeZone.UTC)
    Weeks.weeksBetween(from, to).getWeeks;
  }

  def getDayRange(count: Int): Range = {
    val endDate = DateTime.now(DateTimeZone.UTC);
    val startDate = endDate.minusDays(count);
    Range(dayPeriod.print(startDate).toInt, dayPeriod.print(endDate).toInt)
  }

  def getMonthRange(count: Int): Range = {
    val endDate = DateTime.now(DateTimeZone.UTC);
    val startMonth = endDate.minusMonths(count);
    Range(monthPeriod.print(startMonth).toInt, monthPeriod.print(endDate).toInt)
  }

  def errorResponse(apiId: String, err: String, responseCode: String): Response = {
    Response(apiId, "1.0", df.print(System.currentTimeMillis()),
      Params(UUID.randomUUID().toString, null, responseCode, "failed", err),
      responseCode, None)
  }

  def experimentErrorResponse(apiId: String, errResponse: Map[String, String], responseCode: String): ExperimentBodyResponse = {
    ExperimentBodyResponse(apiId, "1.0", df.print(System.currentTimeMillis()),
      ExperimentParams(UUID.randomUUID().toString, null, responseCode, "failed", errResponse),
      responseCode, None)
  }

  def experimentOkResponse(apiId: String, result: Map[String, AnyRef]): ExperimentBodyResponse = {
    ExperimentBodyResponse(apiId, "1.0", df.print(DateTime.now(DateTimeZone.UTC).getMillis), ExperimentParams(UUID.randomUUID().toString(), null, null, "successful", null), ResponseCode.OK.toString(), Option(result));
  }

  def errorResponseSerialized(apiId: String, err: String, responseCode: String): String = {
    JSONUtils.serialize(errorResponse(apiId, err, responseCode))
  }

  def reportErrorResponse(apiId: String, errResponse: Map[String, String], responseCode: String): Response = {
    Response(apiId, "1.0", df.print(System.currentTimeMillis()),
      Params(UUID.randomUUID().toString, null, responseCode, "failed", null),
      responseCode, Some(errResponse))
  }

  def OK(apiId: String, result: Map[String, AnyRef]): Response = {
    Response(apiId, "1.0", df.print(DateTime.now(DateTimeZone.UTC).getMillis), Params(UUID.randomUUID().toString(), null, null, "successful", null), ResponseCode.OK.toString(), Option(result));
  }

  def getRemainingHours(): Long = {
    val now = DateTime.now(DateTimeZone.UTC);
    new Duration(now, now.plusDays(1).withTimeAtStartOfDay()).getStandardHours;
  }

  def getToday(): String = {
    dateFormat.print(new DateTime)
  }

  def getPreviousDay(): String = {
    dateFormat.print(new DateTime().minusDays(1))
  }

  def getPeriod(date: String): Int = {
    try {
      Integer.parseInt(date.replace("-", ""))
    } catch {
      case t: Throwable => 0; // TODO: handle error
    }
  }

  def getDaysBetween(start: String, end: String): Int = {
    val to = dateFormat.parseLocalDate(end)
    val from = dateFormat.parseLocalDate(start)
    Days.daysBetween(from, to).getDays()
  }

  def caseClassToMap(ccObj: AnyRef) =
    (Map[String, AnyRef]() /: ccObj.getClass.getDeclaredFields) {
      (map, field) =>
        field.setAccessible(true)
        map + (field.getName -> field.get(ccObj))
    }

  // parse query interval for public exhaust APIs
  def getIntervalRange(period: String): DateRange = {
    // LastDay, LastWeek, LastMonth, Last2Days, Last7Days, Last30Days
    period match {
      case "LAST_DAY"    => getDayInterval(1);
      case "LAST_2_DAYS"  => getDayInterval(2);
      case "LAST_7_DAYS"  => getDayInterval(7);
      case "LAST_14_DAYS"  => getDayInterval(14);
      case "LAST_30_DAYS" => getDayInterval(30);
      case "LAST_WEEK"   => getWeekInterval(1);
      case _            => DateRange("", "");
    }
  }

  def getAvailableIntervals(): List[String] = {
    List("LAST_DAY", "LAST_2_DAYS", "LAST_7_DAYS", "LAST_14_DAYS", "LAST_30_DAYS", "LAST_WEEK")
  }

  def getDayInterval(count: Int): DateRange = {
    val endDate = DateTime.now(DateTimeZone.UTC).withTimeAtStartOfDay().plus(offset);
    val startDate = endDate.minusDays(count).toString("yyyy-MM-dd");
    DateRange(startDate, endDate.toString("yyyy-MM-dd"))
  }

  def getMonthInterval(count: Int): DateRange = {
    val currentDate = DateTime.now(DateTimeZone.UTC).withTimeAtStartOfDay().plus(offset);
    val startDate = currentDate.minusDays(count * 30).dayOfMonth().withMinimumValue().toString("yyyy-MM-dd");
    val endDate = currentDate.dayOfMonth().withMinimumValue().toString("yyyy-MM-dd");
    DateRange(startDate, endDate)
  }

  def getWeekInterval(count: Int): DateRange = {
    val currentDate = DateTime.now(DateTimeZone.UTC).withTimeAtStartOfDay().plus(offset);
    val startDate = currentDate.minusDays(count * 7).dayOfWeek().withMinimumValue().toString("yyyy-MM-dd")
    val endDate = currentDate.dayOfWeek().withMinimumValue().toString("yyyy-MM-dd");
    DateRange(startDate, endDate)
  }
}
