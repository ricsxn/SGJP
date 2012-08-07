#!/bin/sh
#
# Script to clean the Science Gateway Job Perusal database
#

CUTOFF_DATE='2012-06-01'


TRACKED_JOBS=$(mktemp)
QUERY="select job_tracking_id from sgjp_tracked_jobs where start_ts < '$CUTOFF_DATE'";
mysql -u sgjp_user -psgjp_password sgjp -s -N -e "$QUERY" > $TRACKED_JOBS

job_records=0
while read job_tracking_id 
do
  printf "processing job_tracking_id: "$job_tracking_id" ... "

  ## logs
  QUERY="delete from sgjp_logs where job_tracking_id=$job_tracking_id;"
  mysql -u sgjp_user -psgjp_password sgjp -s -N -e "$QUERY"
  RES=$?
  if [ $RES -ne 0 ]; then
    echo "ko"
    echo "ERROR while executing: "$QUERY
    continue
  fi
 
  ## snapshots
  QUERY="delete from sgjp_snapshots where job_tracking_id=$job_tracking_id;"
  mysql -u sgjp_user -psgjp_password sgjp -s -N -e "$QUERY"
  RES=$?
  if [ $RES -ne 0 ]; then
    echo "ko"
    echo "ERROR while executing: "$QUERY
    continue
  fi

  ## job_files
  QUERY="delete from sgjp_job_files where job_tracking_id=$job_tracking_id;" 
  mysql -u sgjp_user -psgjp_password sgjp -s -N -e "$QUERY"
  RES=$?
  if [ $RES -ne 0 ]; then
    echo "ko"
    echo "ERROR while executing: "$QUERY
    continue
  fi

  ## job_info
  QUERY="delete from sgjp_job_info where job_tracking_id=$job_tracking_id;"
  mysql -u sgjp_user -psgjp_password sgjp -s -N -e "$QUERY"
  RES=$?
  if [ $RES -ne 0 ]; then
    echo "ko"
    echo "ERROR while executing: "$QUERY
    continue
  fi

  ## job_tracked
  QUERY="delete from sgjp_tracked_jobs where job_tracking_id=$job_tracking_id;"
  mysql -u sgjp_user -psgjp_password sgjp -s -N -e "$QUERY"
  RES=$?
  if [ $RES -ne 0 ]; then
    echo "ko"
    echo "ERROR while executing: "$QUERY
    continue
  fi

  echo "ok"
  # Increase counter
  job_records=$((job_records+1))
done < $TRACKED_JOBS
rm -f $TRACKED_JOBS
echo "Processed "$job_records" job records"
echo 

