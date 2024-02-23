# Data Uploader - Kafka version
# Upload POS batches to a Kafka cluster

# You must have the Confluent CLI installed and configured. 
./check_confluent_cli.sh || exit 1

# Configuration
: "${CLUSTER_ID:=lkc-7p0wzp}" # Set this to your cluster ID
: "${TOPIC:=batched_scans}" 
: "${DATA_DIR:=/tmp/datagen/$TOPIC}"
: "${PREFIX:=$TOPIC}"
: "${SLEEP:=30}" # Time to sleep in between batches

# The status file both logs activity and controls script running
STATUS_FILE=$DATA_DIR/.status
echo "EPOCH" `date` > $STATUS_FILE

# Prechecks for data directories
if [ ! -d $DATA_DIR ]; then
	echo "Please ensure that $DATA_DIR is writable."
  exit 1
fi
if [ ! -d $DATA_DIR/stage ]; then
  echo "creating $DATA_DIR/stage directory"
  mkdir $DATA_DIR/stage
fi
if [ ! -d $DATA_DIR/processed ]; then
  echo "creating $DATA_DIR/processed directory"
  mkdir $DATA_DIR/processed
fi

# Function to print to console and status file
log_message () {
  echo $*
  echo $* >> $STATUS_FILE
}

# When user wants to exit, shut down gracefully
cleanup ()
{
  log_message "STOP" `date`
  exit 0
}
trap cleanup SIGINT SIGTERM
log_message "START" $THREAD `date`

# Upload files generated to Kafka and archive
while [ `grep -c "^STOP" $STATUS_FILE` -eq 0 ] 
do
  # Check if there are any .json files in the stage directory
  files=(${DATA_DIR}/stage/*.json)
  if [ -e "${files[0]}" ]; then
    # Upload files to Kafka and archive
    for staged_file in "${files[@]}"
    do
      output=`confluent kafka topic produce $TOPIC --cluster $CLUSTER_ID 2>&1 < $staged_file`
      if [ $? -ne 0 ]; then
        log_message "ERR_" $THREAD "Upload error" `date` `echo $output | tr -d '\n'`
        exit;
      fi
      log_message "SENT" $staged_file `date` 
      mv $staged_file $DATA_DIR/processed
    done
  fi
  sleep $SLEEP
done
log_message "EXIT" $THREAD `date`
