#!/usr/bin/env bash

export SPARK_HOME=/mount/data/analytics/spark-2.4.4-bin-hadoop2.7
export MODELS_HOME=/mount/data/analytics/models-2.0
export DP_LOGS=/mount/data/analytics/logs/data-products
## Job to run daily
cd /mount/data/analytics/scripts
source model-config.sh
today=$(date "+%Y-%m-%d")

libs_path="/mount/data/analytics/models-2.0/data-products-1.0"

get_report_job_model_name(){
	case "$1" in
		"course-enrollment-report") echo 'org.sunbird.analytics.job.report.CourseEnrollmentJob'
		;;
		"course-consumption-report") echo 'org.sunbird.analytics.job.report.CourseConsumptionJob'
		;;
		"funnel-report") echo 'org.sunbird.analytics.sourcing.FunnelReport'
		;;
		"sourcing-metrics") echo 'org.sunbird.analytics.sourcing.SourcingMetrics'
		;;
		"admin-geo-reports") echo 'org.sunbird.analytics.job.report.StateAdminGeoReportJob'
		;;
		"etb-metrics") echo 'org.sunbird.analytics.job.report.ETBMetricsJob'
		;;
		"admin-user-reports") echo 'org.sunbird.analytics.job.report.StateAdminReportJob'
		;;
		"userinfo-exhaust") echo 'org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob'
		;;
		"response-exhaust") echo 'org.sunbird.analytics.exhaust.collection.ResponseExhaustJob'
		;;
		"progress-exhaust") echo 'org.sunbird.analytics.exhaust.collection.ProgressExhaustJob'
		;;
		"cassandra-migration") echo 'org.sunbird.analytics.updater.CassandraMigratorJob'
        ;;
		"collection-summary-report") echo 'org.sunbird.analytics.job.report.CollectionSummaryJob'
		;;
		"program-collection-summary-report") echo 'org.sunbird.analytics.job.report.CollectionSummaryJob'
		;;
		"collection-summary-report-v2") echo 'org.sunbird.analytics.job.report.CollectionSummaryJobV2'
		;;
		"course-batch-status-updater") echo 'org.sunbird.analytics.audit.CourseBatchStatusUpdaterJob'
		;;
		"collection-reconciliation-job") echo 'org.sunbird.analytics.audit.CollectionReconciliationJob'
		;;
		"assessment-correction") echo 'org.sunbird.analytics.job.report.AssessmentCorrectionJob'
        ;;
		*) echo $1
		;;
	esac
}

if [ ! -z "$1" ]; then job_id=$(get_report_job_model_name $1); fi

if [ ! -z "$1" ]; then job_config=$(config $1 $2); else job_config="$2"; fi

if [ ! -z "$2" ]; then batchIds=";$2"; else batchIds=""; fi


echo "Starting the job - $1" >> "$DP_LOGS/$today-job-execution.log"

echo "Job modelName - $job_id" >> "$DP_LOGS/$today-job-execution.log"

nohup $SPARK_HOME/bin/spark-submit --master local[*] --jars $(echo ${libs_path}/lib/*.jar | tr ' ' ','),$MODELS_HOME/analytics-framework-2.0.jar,$MODELS_HOME/scruid_2.11-2.4.0.jar,$MODELS_HOME/batch-models-2.0.jar --class org.ekstep.analytics.job.JobExecutor $MODELS_HOME/batch-models-2.0.jar --model "$job_id" --config "$job_config$batchIds" >> "$DP_LOGS/$today-job-execution.log" 2>&1

echo "Job execution completed - $1" >> "$DP_LOGS/$today-job-execution.log"