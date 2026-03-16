package org.ekstep.analytics.util

import org.ekstep.analytics.framework.conf.AppConf

object Constants {

    val env = AppConf.getConfig("cassandra.keyspace_prefix");
    val KEY_SPACE_NAME = env+"learner_db";
    val DEVICE_KEY_SPACE_NAME = env+"device_db";
    val DEVICE_SPECIFICATION_TABLE = "device_specification";
    val CONTENT_KEY_SPACE_NAME = env+"content_db";
    val PLATFORM_KEY_SPACE_NAME = env+"platform_db";
    val CONTENT_STORE_KEY_SPACE_NAME = env+"content_store";
    val CONTENT_DATA_TABLE = "content_data";
    val JOB_REQUEST = "job_request";
    val REGISTERED_TAGS = "registered_tags";
    val JOB_CONFIG = "job_config";
    val WORKFLOW_USAGE_SUMMARY = "workflow_usage_summary";
    val WORKFLOW_USAGE_SUMMARY_FACT = "workflow_usage_summary_fact";
    val DEVICE_PROFILE_TABLE = env+"device_profile";
    val EXPERIMENT_DEFINITION_TABLE = env+"experiment_definition";
    val DIALCODE_USAGE_METRICS_TABLE = "dialcode_usage_metrics"
    val DRUID_REPORT_CONFIGS_DEFINITION_TABLE = env+"report_config";

    val DEFAULT_APP_ID = "EkstepPortal";

    val LP_URL = AppConf.getConfig("lp.url")
    val SEARCH_SERVICE_URL = AppConf.getConfig("service.search.url")
    val COMPOSITE_SEARCH_URL = s"$SEARCH_SERVICE_URL" + AppConf.getConfig("service.search.path")
    val ORG_SEARCH_URL: String = AppConf.getConfig("org.search.api.url") + AppConf.getConfig("org.search.api.path")
    val ORG_SEARCH_API_KEY: String = AppConf.getConfig("org.search.api.key")
    val USER_SEARCH_URL : String = AppConf.getConfig("user.search.api.url")

    val HIERARCHY_STORE_KEY_SPACE_NAME = AppConf.getConfig("cassandra.hierarchy_store_prefix")+"hierarchy_store"
    val CONTENT_HIERARCHY_TABLE = "content_hierarchy"

    val ELASTIC_SEARCH_SERVICE_ENDPOINT = AppConf.getConfig("elasticsearch.service.endpoint")
    val ELASTIC_SEARCH_INDEX_COMPOSITESEARCH_NAME = AppConf.getConfig("elasticsearch.index.compositesearch.name")
    val ELASTIC_SEARCH_INDEX_COURSEBATCH_NAME = AppConf.getConfig("elasticsearch.index.coursebatch.name")
    val SUNBIRD_COURSES_KEY_SPACE = AppConf.getConfig("course.metrics.cassandra.sunbirdCoursesKeyspace")
}