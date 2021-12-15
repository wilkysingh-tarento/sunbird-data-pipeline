#!/usr/bin/env bash
export SPARK_HOME=/mount/data/analytics/spark-2.4.4-bin-hadoop2.7
export MODELS_HOME=/mount/data/analytics/models-2.0
export DP_LOGS=/mount/data/analytics/logs/data-products
export SERVICE_LOGS=/mount/data/analytics/logs/services
export JM_HOME=/mount/data/analytics/job-manager

export azure_storage_key=igot
export azure_storage_secret=4WCx35Qb3qnQOgkrkjkOz6shiJ264idVRFJc6HErwfyaR5V/weC7n+7tW4bz5NdcEifWeVT3W9dmZxTokZ7uAw==
export reports_azure_storage_key=igot
export reports_azure_storage_secret=4WCx35Qb3qnQOgkrkjkOz6shiJ264idVRFJc6HErwfyaR5V/weC7n+7tW4bz5NdcEifWeVT3W9dmZxTokZ7uAw==
export druid_storage_account_key=igot
export druid_storage_account_secret=4WCx35Qb3qnQOgkrkjkOz6shiJ264idVRFJc6HErwfyaR5V/weC7n+7tW4bz5NdcEifWeVT3W9dmZxTokZ7uAw==

export heap_conf_str="-XX:+UseG1GC -XX:MaxGCPauseMillis=100 -Xms250m -Xmx5120m -XX:+UseStringDeduplication"
today=$(date "+%Y-%m-%d")

kill_job_manager()
{
    echo "Killing currently running job-manager process" >> "$SERVICE_LOGS/$today-job-manager.log"
    kill $(ps aux | grep '[j]ob-manager' | awk '{print $2}') >> "$SERVICE_LOGS/$today-job-manager.log"
}

start_job_manager()
{
    kill_job_manager # Before starting the job, We are killing the job-manager
    cd /mount/data/analytics/scripts
    source model-config.sh
    job_config=$(config 'job-manager')
    echo "Starting the job manager" >> "$SERVICE_LOGS/$today-job-manager.log"
    echo "config: $job_config" >> "$SERVICE_LOGS/$today-job-manager.log"
    nohup java $heap_conf_str -cp "$SPARK_HOME/jars/*:$MODELS_HOME/*:$MODELS_HOME/data-products-1.0/lib/*" -Dconfig.file=$MODELS_HOME/dev.conf org.ekstep.analytics.job.JobManager --config "$job_config" >> $SERVICE_LOGS/$today-job-manager.log 2>&1 &

    job_manager_pid=$(ps aux | grep '[j]ob-manager' | awk '{print $2}') # Once Job is started just we are making whether job is running or not.
    if [[ ! -z "$job_manager_pid" ]]; then
        echo "Job manager is started." >> "$SERVICE_LOGS/$today-job-manager.log"
    else
        echo "Job manager is not started." >> "$SERVICE_LOGS/$today-job-manager.log"
    fi
}
# Tasks
# Kill the job-manager
# Start the job-manager
# Make sure whether is running or not.
start_job_manager
