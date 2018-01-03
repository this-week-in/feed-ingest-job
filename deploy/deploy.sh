#!/usr/bin/env bash

APP_NAME=feed-ingest-job
JOB_NAME=${APP_NAME}
SCHEDULER_SERVICE_NAME=scheduler-joshlong

cf push -b java_buildpack --health-check-type none --no-route  -p target/${APP_NAME}.jar ${APP_NAME}

# scheduler
cf s | grep ${SCHEDULER_SERVICE_NAME} || cf cs scheduler-for-pcf standard ${SCHEDULER_SERVICE_NAME}
cf bs ${APP_NAME} ${SCHEDULER_SERVICE_NAME}

REDIS_NAME=feed-ingest-cache
cf s | grep ${REDIS_NAME} || cf cs rediscloud 100mb ${REDIS_NAME}
cf bs ${APP_NAME} ${REDIS_NAME}

cf set-env ${APP_NAME} PINBOARD_TOKEN ${PINBOARD_TOKEN}

cf restage ${APP_NAME}

cf delete-job -f ${JOB_NAME}
cf create-job ${APP_NAME} ${JOB_NAME} ".java-buildpack/open_jdk_jre/bin/java org.springframework.boot.loader.JarLauncher"
cf run-job ${JOB_NAME}
cf schedule-job ${JOB_NAME} "*/5 * ? * *"
#cf schedule-job $JOB_NAME "0 1 ? * *"
