package org.ekstep.analytics.framework.util

import java.io._
import java.net.URL
import java.nio.file.Paths.get
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.security.MessageDigest
import java.sql.Timestamp
import java.util.{Date, Properties}
import java.util.zip.GZIPOutputStream

import ing.wbaa.druid.definitions.{Granularity, GranularityType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.{DtRange, Event, JobConfig, _}

import scala.collection.mutable.ListBuffer
//import org.ekstep.analytics.framework.conf.AppConf
import java.util.zip.{ZipEntry, ZipOutputStream}

import org.apache.commons.lang3.StringUtils
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, DateTimeZone, Days, LocalDate, Weeks, Years}
import org.sunbird.cloud.storage.conf.AppConf

import scala.util.control.Breaks._

object CommonUtil {

  implicit val className = "org.ekstep.analytics.framework.util.CommonUtil"
  @transient val df3: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZZ").withZoneUTC();
  @transient val df5: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").withZoneUTC();
  @transient val df6: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").withZoneUTC();
  @transient val ISTDateTimeFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.forID("Asia/Kolkata"))
  @transient val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC();
  @transient val weekPeriodLabel: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-ww").withZoneUTC();
  @transient val dayPeriod: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
  @transient val monthPeriod: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMM").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
  @transient val dayPeriodFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC();

  def getParallelization(config: JobConfig): Int = {

    val defParallelization = AppConf.getConfig("default.parallelization").toInt;
    config.parallelization.getOrElse(defParallelization);
  }

  def getFrameworkContext(storageServices: Option[Array[(String, String, String)]]): FrameworkContext = {
    val fc = new FrameworkContext();
    fc.initialize(storageServices)
    fc;
  }

  def getSparkContext(parallelization: Int, appName: String, sparkCassandraConnectionHost: Option[AnyRef] = None, sparkElasticsearchConnectionHost: Option[AnyRef] = None): SparkContext = {
    JobLogger.log("Initializing Spark Context")
    val conf = new SparkConf().setAppName(appName).set("spark.default.parallelism", parallelization.toString)
      .set("spark.driver.memory", AppConf.getConfig("spark.driver_memory"))
      .set("spark.memory.fraction", AppConf.getConfig("spark.memory_fraction"))
      .set("spark.memory.storageFraction", AppConf.getConfig("spark.storage_fraction"))
    val master = conf.getOption("spark.master")
    // $COVERAGE-OFF$ Disabling scoverage as the below code cannot be covered as they depend on environment variables
    if (master.isEmpty) {
      JobLogger.log("Master not found. Setting it to local[*]")
      conf.setMaster("local[*]")
    }

    if (!conf.contains("spark.cassandra.connection.host"))
      conf.set("spark.cassandra.connection.host", AppConf.getConfig("spark.cassandra.connection.host"))

    if (sparkCassandraConnectionHost.nonEmpty) {
      conf.set("spark.cassandra.connection.host", sparkCassandraConnectionHost.get.asInstanceOf[String])
      println("setting spark.cassandra.connection.host to lp-cassandra", conf.get("spark.cassandra.connection.host"))
    }

    if (sparkElasticsearchConnectionHost.nonEmpty) {
      conf.set("es.nodes", sparkElasticsearchConnectionHost.get.asInstanceOf[String])
      conf.set("es.port", "9200")
      conf.set("es.write.rest.error.handler.log.logger.name", "org.ekstep.es.dispatcher")
      conf.set("es.write.rest.error.handler.log.logger.level", "INFO")
    }

    // $COVERAGE-ON$
    val sc = new SparkContext(conf)
    setS3Conf(sc)
    setAzureConf(sc)
    JobLogger.log("Spark Context initialized")
    sc
  }

  def getSparkSession(parallelization: Int, appName: String, sparkCassandraConnectionHost: Option[AnyRef] = None,
                      sparkElasticsearchConnectionHost: Option[AnyRef] = None, readConsistencyLevel: Option[String] = None): SparkSession = {
    JobLogger.log("Initializing SparkSession")
    val conf = new SparkConf().setAppName(appName).set("spark.default.parallelism", parallelization.toString)
      .set("spark.driver.memory", AppConf.getConfig("spark.driver_memory"))
      .set("spark.memory.fraction", AppConf.getConfig("spark.memory_fraction"))
      .set("spark.memory.storageFraction", AppConf.getConfig("spark.storage_fraction"))
    val master = conf.getOption("spark.master")
    // $COVERAGE-OFF$ Disabling scoverage as the below code cannot be covered as they depend on environment variables
    if (master.isEmpty) {
      JobLogger.log("Master not found. Setting it to local[*]")
      conf.setMaster("local[*]")
    }

    if (!conf.contains("spark.cassandra.connection.host"))
      conf.set("spark.cassandra.connection.host", AppConf.getConfig("spark.cassandra.connection.host"))
    if (embeddedCassandraMode)
      conf.set("spark.cassandra.connection.port", AppConf.getConfig("cassandra.service.embedded.connection.port"))

    if (sparkCassandraConnectionHost.nonEmpty) {
      conf.set("spark.cassandra.connection.host", sparkCassandraConnectionHost.get.asInstanceOf[String])
      if (readConsistencyLevel.nonEmpty) {
        conf.set("spark.cassandra.input.consistency.level", readConsistencyLevel.get);
      }
      println("setting spark.cassandra.connection.host to lp-cassandra", conf.get("spark.cassandra.connection.host"))
    }

    if (sparkElasticsearchConnectionHost.nonEmpty) {
      conf.set("es.nodes", sparkElasticsearchConnectionHost.get.asInstanceOf[String])
      conf.set("es.port", "9200")
      conf.set("es.write.rest.error.handler.log.logger.name", "org.ekstep.es.dispatcher")
      conf.set("es.write.rest.error.handler.log.logger.level", "INFO")
      conf.set("es.write.operation", "upsert")

    }

    // $COVERAGE-ON$
    val sparkSession = SparkSession.builder().appName("sunbird-analytics").config(conf).getOrCreate()
    setS3Conf(sparkSession.sparkContext)
    setAzureConf(sparkSession.sparkContext)
    JobLogger.log("SparkSession initialized")
    sparkSession
  }

  private def embeddedCassandraMode(): Boolean = {
    val isEmbedded = AppConf.getConfig("cassandra.service.embedded.enable");
    StringUtils.isNotBlank(isEmbedded) && StringUtils.equalsIgnoreCase("true", isEmbedded);
  }

  def setS3Conf(sc: SparkContext) = {
    JobLogger.log("Configuring S3 AccessKey& SecrateKey to SparkContext")
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AppConf.getAwsKey());
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", AppConf.getAwsSecret());
  }

  def setAzureConf(sc: SparkContext) = {
    val accName = AppConf.getStorageKey("azure")
    val accKey = AppConf.getStorageSecret("azure")
    sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    sc.hadoopConfiguration.set("fs.azure.account.key." + accName + ".blob.core.windows.net", accKey)
  }

  def closeSparkContext()(implicit sc: SparkContext) {
    JobLogger.log("Closing Spark Context", None, INFO)
    sc.stop();
  }

  class Visitor extends java.nio.file.SimpleFileVisitor[java.nio.file.Path] {
    override def visitFile(
      file:  java.nio.file.Path,
      attrs: java.nio.file.attribute.BasicFileAttributes): java.nio.file.FileVisitResult =
      {
        Files.delete(file);

        java.nio.file.FileVisitResult.CONTINUE;
      } // visitFile

    override def postVisitDirectory(
      dir: java.nio.file.Path,
      exc: IOException): java.nio.file.FileVisitResult =
      {
        Files.delete(dir);
        java.nio.file.FileVisitResult.CONTINUE;
      } // visitFile
  }

  def deleteDirectory(dir: String) {
    val path = get(dir);
    val directory = new File(dir)
    if (directory.exists()) {
      JobLogger.log("Deleting directory", Option(path.toString()))
      Files.walkFileTree(path, new Visitor());
    } else {
      JobLogger.log("Directory not exists", Option(path.toString()))
    }
  }

  def createDirectory(dir: String) {
    val path = get(dir);
    JobLogger.log("Creating directory", Option(path.toString()))
    Files.createDirectories(path);
  }

  def copyFile(from: InputStream, path: String, fileName: String) = {
    createDirectory(path);
    Files.copy(from, Paths.get(path + fileName), StandardCopyOption.REPLACE_EXISTING);
  }

  def deleteFile(file: String) {
    JobLogger.log("Deleting file ", Option(file))
    val path = get(file);
    if (Files.exists(path))
      Files.delete(path);
  }

  def datesBetween(from: LocalDate, to: LocalDate): IndexedSeq[LocalDate] = {
    val numberOfDays = Days.daysBetween(from, to).getDays()
    for (f <- 0 to numberOfDays) yield from.plusDays(f)
  }

  def getStartDate(endDate: Option[String], delta: Int): Option[String] = {
    val to = if (endDate.nonEmpty) dateFormat.parseLocalDate(endDate.get) else LocalDate.fromDateFields(new Date);
    Option(to.minusDays(delta).toString());
  }

  def getDatesBetween(fromDate: String, toDate: Option[String], pattern: String): Array[String] = {
    val df: DateTimeFormatter = DateTimeFormat.forPattern(pattern).withZoneUTC();
    val to = if (toDate.nonEmpty) df.parseLocalDate(toDate.get) else LocalDate.fromDateFields(new Date);
    val from = df.parseLocalDate(fromDate);
    val dates = datesBetween(from, to);
    dates.map { x => df.print(x) }.toArray;
  }

  def getDatesBetween(fromDate: String, toDate: Option[String]): Array[String] = {
    getDatesBetween(fromDate, toDate, "yyyy-MM-dd");
  }

  def daysBetween(from: LocalDate, to: LocalDate): Int = {
    Days.daysBetween(from, to).getDays();
  }

  def getEventTS(event: Event): Long = {
    if (event.ets > 0)
      event.ets
    else
      getTimestamp(event.ts);
  }

  def getEventSyncTS(event: Event): Long = {
    val timeInString = event.`@timestamp`;
    getEventSyncTS(timeInString);
  }

  def getEventSyncTS(event: V3Event): Long = {
    val timeInString = event.`@timestamp`;
    getEventSyncTS(timeInString);
  }

  def getEventSyncTS(timeInStr: String): Long = {
    var ts = getTimestamp(timeInStr, df5, "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    if (ts == 0) {
      ts = getTimestamp(timeInStr, df3, "yyyy-MM-dd'T'HH:mm:ssZZ");
    }
    if (ts == 0) {
      try {
        ts = getTimestamp(timeInStr.substring(0, 19), df6, "yyyy-MM-dd'T'HH:mm:ss");
      } catch {
        case ex: Exception =>
          ts = 0L;
      }
    }
    ts;
  }

  def getEventDate(event: Event): Date = {
    try {
      df3.parseLocalDate(event.ts).toDate;
    } catch {
      case _: Exception =>
        JobLogger.log("Invalid event time", Option(Map("ts" -> event.ts)));
        null;
    }
  }

  def getGameId(event: Event): String = {
    if (event.gdata != null) event.gdata.id else null;
  }

  def getGameVersion(event: Event): String = {
    if (event.gdata != null) event.gdata.ver else null;
  }

  def getGameId(event: V3Event): String = {
    if (event.`object`.isEmpty) null else event.`object`.get.id;
  }

  def getGameVersion(event: V3Event): String = {
    if (event.`object`.isEmpty) null else event.`object`.get.ver.getOrElse(null);
  }

  def getParallelization(config: Option[Map[String, String]]): Int = {
    getParallelization(config.getOrElse(Map[String, String]()));
  }

  def getParallelization(config: Map[String, String]): Int = {
    var parallelization = AppConf.getConfig("default.parallelization");
    if (config != null && config.nonEmpty) {
      parallelization = config.getOrElse("parallelization", parallelization);
    }
    parallelization.toInt;
  }

  def gzip(path: String): String = {
    val buf = new Array[Byte](1024);
    val src = new File(path);
    val dst = new File(path ++ ".gz");

    try {
      val in = new BufferedInputStream(new FileInputStream(src))
      try {
        val out = new GZIPOutputStream(new FileOutputStream(dst))
        try {
          var n = in.read(buf)
          while (n > 0) {
            out.write(buf, 0, n)
            n = in.read(buf)
          }
        } finally {
          out.finish();
          out.close();
        }
      } finally {
        in.close();
      }
    } catch {
      case e: Exception =>
        JobLogger.log(e.getMessage, None, ERROR)
        throw e
    }
    path ++ ".gz";
  }

  def zip(out: String, files: Iterable[String]) = {
    import java.io.{BufferedInputStream, FileInputStream, FileOutputStream}
    import java.util.zip.{ZipEntry, ZipOutputStream}

    val zip = new ZipOutputStream(new FileOutputStream(out))

    files.foreach { name =>
      zip.putNextEntry(new ZipEntry(name.split("/").last))
      val in = new BufferedInputStream(new FileInputStream(name))
      var b = in.read()
      while (b > -1) {
        zip.write(b)
        b = in.read()
      }
      in.close()
      zip.closeEntry()
    }
    zip.close()
  }

  def zipFolder(outFile: String, dir: String) = {
    import java.io.{BufferedInputStream, FileInputStream, FileOutputStream}
    import java.util.zip.{ZipEntry, ZipOutputStream}

    val zip = new ZipOutputStream(new FileOutputStream(outFile))
    val files = new File(dir).listFiles();
    files.foreach { file =>
      zip.putNextEntry(new ZipEntry(file.getName.split("/").last))
      val in = new BufferedInputStream(new FileInputStream(file))
      var b = in.read()
      while (b > -1) {
        zip.write(b)
        b = in.read()
      }
      in.close()
      zip.closeEntry()
    }
    zip.close()
  }

  // zipping nested directories
  def zipDir(zipFileName: String, dir: String) {
    val dirObj = new File(dir);
    val out = new ZipOutputStream(new FileOutputStream(zipFileName));
    addDir(dirObj, out);
    out.close();
  }
  def addDir(dirObj: File, out: ZipOutputStream, basePath: String = "") {
    val files = dirObj.listFiles();
    val tmpBuf = new Array[Byte](1024);

    for (file <- files) {
      breakable {
        if (file.isDirectory()) {
          addDir(file, out, basePath + file.getName.split("/").last + "/");
          break;
        } else {
          val in = new FileInputStream(file.getAbsolutePath());
          out.putNextEntry(new ZipEntry(basePath + file.getName.split("/").last));
          var len: Int = in.read(tmpBuf);
          while (len > 0) {
            out.write(tmpBuf, 0, len);
            len = in.read(tmpBuf);
          }
          out.closeEntry();
          in.close();
        }
      }
    }
  }

  def getAge(dob: Date): Int = {
    val birthdate = LocalDate.fromDateFields(dob);
    val now = new LocalDate();
    val age = Years.yearsBetween(birthdate, now);
    age.getYears;
  }

  def getTimeSpent(len: AnyRef): Option[Double] = {
    if (null != len) {
      if (len.isInstanceOf[String]) {
        Option(len.asInstanceOf[String].toDouble)
      } else if (len.isInstanceOf[Double]) {
        Option(len.asInstanceOf[Double])
      } else if (len.isInstanceOf[Int]) {
        Option(len.asInstanceOf[Int].toDouble)
      } else {
        Option(0d);
      }
    } else {
      Option(0d);
    }
  }

  def getTimeDiff(start: Event, end: Event): Option[Double] = {

    val st = getTimestamp(start.ts);
    val et = getTimestamp(end.ts);
    if (et == 0 || st == 0) {
      Option(0d);
    } else {
      Option(roundDouble(((et - st).toDouble / 1000), 2));
    }
  }

  def getTimeDiff(start: Long, end: Long): Option[Double] = {

    Option((end - start).toDouble / 1000);
  }

  def getHourOfDay(start: Long, end: Long): ListBuffer[Int] = {
    val hrList = ListBuffer[Int]();
    val startHr = new DateTime(start, DateTimeZone.UTC).getHourOfDay;
    val endHr = new DateTime(end, DateTimeZone.UTC).getHourOfDay;
    var hr = startHr;
    while (hr != endHr) {
      hrList += hr;
      hr = hr + 1;
      if (hr == 24) hr = 0;
    }
    hrList += endHr;
  }

  def getTimestamp(ts: String, df: DateTimeFormatter, pattern: String): Long = {
    try {
      df.parseDateTime(ts).getMillis;
    } catch {
      case _: Exception =>
        JobLogger.log("Invalid time format", Option(Map("pattern" -> pattern, "ts" -> ts)));
        0;
    }
  }

  def getTimestamp(timeInString: String): Long = {
    var ts = getTimestamp(timeInString, df3, "yyyy-MM-dd'T'HH:mm:ssZZ");
    if (ts == 0) {
      ts = getTimestamp(timeInString, df5, "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    }
    if (ts == 0) {
      try {
        ts = getTimestamp(timeInString.substring(0, 19), df6, "yyyy-MM-dd'T'HH:mm:ss");
      } catch {
        case ex: Exception =>
          ts = 0L;
      }
    }
    ts;
  }

  def getTags(metadata: Map[String, AnyRef]): Option[Array[String]] = {
    val tags = metadata.getOrElse("tags", List[String]());
    if (null == tags) Option(Array[String]()) else Option(tags.asInstanceOf[List[String]].toArray);
  }

  def roundDouble(value: Double, precision: Int): Double = {
    BigDecimal(value).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble;
  }

  def getDefaultAppChannelIds(): (String, String) = {
    (AppConf.getConfig("default.consumption.app.id"), AppConf.getConfig("default.channel.id"))
  }

  def getMessageId(eventId: String, userId: String, granularity: String, dateRange: DtRange, contentId: String = "NA", appId: Option[String] = Option(getDefaultAppChannelIds._1), channelId: Option[String] = Option(getDefaultAppChannelIds._2)): String = {
    val key = Array(eventId, userId, dateRange.from, dateRange.to, granularity, contentId, if (appId.isEmpty) getDefaultAppChannelIds._1 else appId.get, if (channelId.isEmpty) getDefaultAppChannelIds._2 else channelId.get).mkString("|");
    MessageDigest.getInstance("MD5").digest(key.getBytes).map("%02X".format(_)).mkString;
  }

  def getMessageId(eventId: String, userId: String, granularity: String, dateRange: DtRange, contentId: String, appId: Option[String], channelId: Option[String], did: String): String = {
    val key = Array(eventId, userId, contentId, did, if (appId.isEmpty) getDefaultAppChannelIds._1 else appId.get, if (channelId.isEmpty) getDefaultAppChannelIds._2 else channelId.get, did).mkString("|");
    MessageDigest.getInstance("MD5").digest(key.getBytes).map("%02X".format(_)).mkString;
  }

  def getMessageId(eventId: String, userId: String, granularity: String, syncDate: Long, appId: Option[String], channelId: Option[String]): String = {
    val key = Array(eventId, userId, dateFormat.print(syncDate), granularity, if (appId.isEmpty) getDefaultAppChannelIds._1 else appId.get, if (channelId.isEmpty) getDefaultAppChannelIds._2 else channelId.get).mkString("|");
    MessageDigest.getInstance("MD5").digest(key.getBytes).map("%02X".format(_)).mkString;
  }

  def getMessageId(eventId: String, level: String, timeStamp: Long, appId: Option[String], channelId: Option[String]): String = {
    val key = Array(eventId, level, df5.print(timeStamp), if (appId.isEmpty) getDefaultAppChannelIds._1 else appId.get, if (channelId.isEmpty) getDefaultAppChannelIds._2 else channelId.get).mkString("|");
    MessageDigest.getInstance("MD5").digest(key.getBytes).map("%02X".format(_)).mkString;
  }

  def getPeriod(date: DateTime, period: Period): Int = {
    getPeriod(date.getMillis, period);
  }

  def getPeriod(syncts: Long, period: Period): Int = {
    val d = new DateTime(syncts, DateTimeZone.UTC);
    period match {
      case DAY        => dayPeriod.print(d).toInt;
      case WEEK       => getWeekNumber(d.getWeekyear, d.getWeekOfWeekyear)
      case MONTH      => monthPeriod.print(d).toInt;
      case CUMULATIVE => 0
      case LAST7      => 7
      case LAST30     => 30
      case LAST90     => 90
    }
  }

  def getWeeksBetween(fromDate: Long, toDate: Long): Int = {
    val from = new LocalDate(fromDate, DateTimeZone.UTC)
    val to = new LocalDate(toDate, DateTimeZone.UTC)
    Weeks.weeksBetween(from, to).getWeeks;
  }

  private def getWeekNumber(year: Int, weekOfWeekyear: Int): Int = {

    if (weekOfWeekyear < 10) {
      (year + "70" + weekOfWeekyear).toInt
    } else {
      (year + "7" + weekOfWeekyear).toInt
    }
  }

  def time[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    ((t1 - t0), result)
  }

  def getPathFromURL(absUrl: String): String = {
    val url = new URL(absUrl);
    url.getPath
  }

  @throws(classOf[Exception])
  def getPeriods(period: Period, periodUpTo: Int): Array[Int] = {
    period match {
      case DAY        => getDayPeriods(periodUpTo);
      case MONTH      => getMonthPeriods(periodUpTo);
      case WEEK       => getWeekPeriods(periodUpTo);
      case CUMULATIVE => Array(0);
    }
  }

  def getPeriods(periodType: String, periodUpTo: Int): Array[Int] = {
    Period.withName(periodType) match {
      case DAY        => getDayPeriods(periodUpTo);
      case MONTH      => getMonthPeriods(periodUpTo);
      case WEEK       => getWeekPeriods(periodUpTo);
      case CUMULATIVE => Array(0);
    }
  }

  private def getDayPeriods(count: Int): Array[Int] = {
    val endDate = DateTime.now(DateTimeZone.UTC);
    val x = for (i <- 1 to count) yield getPeriod(endDate.minusDays(i), DAY);
    x.toArray;
  }

  private def getMonthPeriods(count: Int): Array[Int] = {
    val endDate = DateTime.now(DateTimeZone.UTC);
    val x = for (i <- 0 to (count - 1)) yield getPeriod(endDate.minusMonths(i), MONTH);
    x.toArray;
  }

  private def getWeekPeriods(count: Int): Array[Int] = {
    val endDate = DateTime.now(DateTimeZone.UTC);
    val x = for (i <- 0 to (count - 1)) yield getPeriod(endDate.minusWeeks(i), WEEK);
    x.toArray;
  }

  def getValidTags(event: Any, registeredTags: Array[String]): Array[String] = {

    val appTag = if (event.isInstanceOf[DerivedEvent]) {
      event.asInstanceOf[DerivedEvent].etags.get.app
    } else if (event.isInstanceOf[Event]) {
      getETags(event.asInstanceOf[Event]).app
    } else if (event.isInstanceOf[V3Event]) {
      getETags(event.asInstanceOf[V3Event]).app
    } else {
      None
    }
    val dimTag = if (event.isInstanceOf[DerivedEvent]) {
      event.asInstanceOf[DerivedEvent].etags.get.dims
    } else if (event.isInstanceOf[Event]) {
      getETags(event.asInstanceOf[Event]).dims
    } else if (event.isInstanceOf[V3Event]) {
      getETags(event.asInstanceOf[V3Event]).dims
    } else {
      None
    }
    val genieTagFilter = if (appTag.isDefined) appTag.get else List()
    val dimTagFilter = if (dimTag.isDefined) dimTag.get else List()
    val tagFilter = genieTagFilter ++ dimTagFilter
    tagFilter.filter { x => registeredTags.contains(x) }.toArray;
  }

  def getValidTagsForWorkflow(event: DerivedEvent, registeredTags: Array[String]): Array[String] = {
    val tagFilter = if (event.tags != null && !event.tags.isEmpty) { event.tags.get.asInstanceOf[List[String]] } else List()
    tagFilter.filter { x => registeredTags.contains(x) }.toArray;
  }

  def caseClassToMap(ccObj: Any) =
    (Map[String, AnyRef]() /: ccObj.getClass.getDeclaredFields) {
      (map, field) =>
        field.setAccessible(true)
        map + (field.getName -> field.get(ccObj))
    }

  def caseClassToMapWithDateConversion(ccObj: AnyRef) =
    (Map[String, AnyRef]() /: ccObj.getClass.getDeclaredFields) {
      (map, field) =>
        field.setAccessible(true)
        map + (field.getName -> (if (field.get(ccObj).isInstanceOf[DateTime]) field.get(ccObj).asInstanceOf[DateTime].getMillis.asInstanceOf[AnyRef] else field.get(ccObj)))
    }

  def getEndTimestampOfDay(date: String): Long = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.forOffsetHoursMinutes(5, 30));
    dateFormat.parseDateTime(date).plusHours(23).plusMinutes(59).plusSeconds(59).getMillis
  }

  def getAppDetails(event: Any): PData = {
    val defaultAppId = PData(AppConf.getConfig("default.consumption.app.id"), "1.0")
    if (event.isInstanceOf[Event]) {
      if (event.asInstanceOf[Event].pdata.nonEmpty && StringUtils.isNotBlank(event.asInstanceOf[Event].pdata.get.id)) event.asInstanceOf[Event].pdata.get else defaultAppId
    } else if (event.isInstanceOf[V3Event]) {
      if (event.asInstanceOf[V3Event].context.pdata.nonEmpty && StringUtils.isNotBlank(event.asInstanceOf[V3Event].context.pdata.get.id)) {
        val v3pdata = event.asInstanceOf[V3Event].context.pdata.get
        PData(v3pdata.id, v3pdata.ver.getOrElse(""), None, v3pdata.pid)
      } else defaultAppId
    } else if (event.isInstanceOf[ProfileEvent]) {
      if (event.asInstanceOf[ProfileEvent].pdata.nonEmpty && StringUtils.isNotBlank(event.asInstanceOf[ProfileEvent].pdata.get.id)) event.asInstanceOf[ProfileEvent].pdata.get else defaultAppId
    } else if (event.isInstanceOf[DerivedEvent]) {
      if (event.asInstanceOf[DerivedEvent].dimensions.pdata.nonEmpty && StringUtils.isNotBlank(event.asInstanceOf[DerivedEvent].dimensions.pdata.get.id)) event.asInstanceOf[DerivedEvent].dimensions.pdata.get else defaultAppId
    } else defaultAppId;
  }

  def getChannelId(event: Any): String = {
    val defaultChannelId = AppConf.getConfig("default.channel.id")
    if (event.isInstanceOf[Event]) {
      if (event.asInstanceOf[Event].channel.nonEmpty && StringUtils.isNotBlank(event.asInstanceOf[Event].channel.get)) event.asInstanceOf[Event].channel.get else defaultChannelId
    } else if (event.isInstanceOf[V3Event]) {
      if (event.asInstanceOf[V3Event].context.channel.nonEmpty && StringUtils.isNotBlank(event.asInstanceOf[V3Event].context.channel)) event.asInstanceOf[V3Event].context.channel else defaultChannelId
    } else if (event.isInstanceOf[DerivedEvent]) {
      if (event.asInstanceOf[DerivedEvent].dimensions.channel.nonEmpty) event.asInstanceOf[DerivedEvent].dimensions.channel.get else if (StringUtils.isBlank(event.asInstanceOf[DerivedEvent].channel)) defaultChannelId else event.asInstanceOf[DerivedEvent].channel
    } else if (event.isInstanceOf[ProfileEvent]) {
      if (event.asInstanceOf[ProfileEvent].channel.nonEmpty && StringUtils.isNotBlank(event.asInstanceOf[ProfileEvent].channel.get)) event.asInstanceOf[ProfileEvent].channel.get else defaultChannelId
    } else defaultChannelId;
  }

  def getETags(event: Event): ETags = {
    if (event.etags.isDefined) {
      event.etags.get;
    } else {
      if (event.tags != null) {
        val tags = event.tags.asInstanceOf[List[Map[String, List[String]]]]
        val genieTags = tags.filter(f => f.contains("genie")).map { x => x.get("genie").get }.flatMap { x => x }
        val partnerTags = tags.filter(f => f.contains("partner")).map { x => x.get("partner").get }.flatMap { x => x }
        val dims = tags.filter(f => f.contains("dims")).map { x => x.get("dims").get }.flatMap { x => x }
        ETags(Option(genieTags), Option(partnerTags), Option(dims))
      } else {
        ETags()
      }

    }
  }

  def getETags(event: V3Event): ETags = {
    if (event.tags != null && !event.tags.isEmpty) {
      val first = event.tags.apply(0)
      if (first.isInstanceOf[String]) {
        ETags(Option(event.tags.asInstanceOf[List[String]]))
      } else {
        val tags = event.tags.asInstanceOf[List[Map[String, List[String]]]]
        val genieTags = tags.filter(f => f.contains("genie")).map { x => x.get("genie").get }.flatMap { x => x }
        val partnerTags = tags.filter(f => f.contains("partner")).map { x => x.get("partner").get }.flatMap { x => x }
        val dims = tags.filter(f => f.contains("dims")).map { x => x.get("dims").get }.flatMap { x => x }
        ETags(Option(genieTags), Option(partnerTags), Option(dims))
      }
    } else {
      ETags()
    }
  }

  def dayPeriodToLong(period: Int): Long = {
    val p = period.toString()
    if (8 == p.length()) {
      val date = p.substring(0, 4) + "-" + p.substring(4, 6) + "-" + p.substring(6)
      dateFormat.parseMillis(date)
    } else 0l;
  }

  def getTimestampOfDayPeriod(period: Int): Long = {
    val periodDateTime = dayPeriodFormat.parseDateTime(period.toString()).withTimeAtStartOfDay()
    periodDateTime.getMillis
  }

  def avg(xs: List[Int]): Float = {
    val (sum, length) = xs.foldLeft((0, 0))({ case ((s, l), x) => (x + s, 1 + l) })
    sum / length
  }

  // parse druid query interval
  def getIntervalRange(period: String): String = {
    // LastDay, LastWeek, LastMonth, Last7Days, Last30Days
    period match {
      case "LastDay"    => getDayRange(1);
      case "LastWeek"   => getWeekRange(1);
      case "LastMonth"  => getMonthRange(1);
      case "Last7Days"  => getDayRange(7);
      case "Last30Days" => getDayRange(30);
      case _            => period;
    }
  }

  def getDayRange(count: Int): String = {
    val endDate = DateTime.now(DateTimeZone.UTC);
    val startDate = endDate.minusDays(count).toString("yyyy-MM-dd");
    startDate + "/" + endDate.toString("yyyy-MM-dd")
  }

  def getMonthRange(count: Int): String = {
    val currentDate = DateTime.now(DateTimeZone.UTC);
    val startDate = currentDate.minusDays(count * 30).dayOfMonth().withMinimumValue().toString("yyyy-MM-dd");
    val endDate = currentDate.dayOfMonth().withMinimumValue().toString("yyyy-MM-dd");
    startDate + "/" + endDate
  }

  def getWeekRange(count: Int): String = {
    val currentDate = DateTime.now(DateTimeZone.UTC);
    val startDate = currentDate.minusDays(count * 7).dayOfWeek().withMinimumValue().toString("yyyy-MM-dd")
    val endDate = currentDate.dayOfWeek().withMinimumValue().toString("yyyy-MM-dd");
    startDate + "/" + endDate
  }

  def getGranularity(value: String): Granularity = {
    GranularityType.decode(value).right.getOrElse(GranularityType.All)
  }

  def getMetricEvent(params: Map[String, AnyRef], producerId: String, producerPid: String): V3DerivedEvent = {

    val channel = "data-pipeline"
    val actorId = "analytics"
    val env = AppConf.getConfig("application.env")
    val measures = params;
    val ts = new DateTime().getMillis
    val mid = CommonUtil.getMessageId("METRIC", producerId + producerPid, ts, None, None);
    val context = V3Context(channel, Option(V3PData(producerId, Option("1.0"), Option(producerPid))), env, None, None, None, None)
    V3DerivedEvent("METRIC", System.currentTimeMillis(), new DateTime().toString(CommonUtil.df3), "3.0", mid, Actor(actorId, "System"), context, None, measures)
  }
  def getTimestampFromEpoch(epochValue: Long): Timestamp = {
    Timestamp.valueOf(new DateTime(epochValue).withZone(DateTimeZone.UTC).toString("yyyy-MM-dd HH:mm:ss.SSS"))
  }

  def getPostgresConnectionProps(): Properties = {
    val user = AppConf.getConfig("postgres.user")
    val pass = AppConf.getConfig("postgres.pass")

    val connProperties = new Properties()
    connProperties.setProperty("driver", "org.postgresql.Driver")
    connProperties.setProperty("user", user)
    connProperties.setProperty("password", pass)
    connProperties
  }
}