# Data Uploader - Kafka version
# Upload POS batches to a Kafka cluster

# You must have the Confluent CLI installed and configured. 
./check_confluent_cli.sh || exit 1

# Configuration
: "${CLUSTER_ID:=lkc-7p0wzp}" # Set this to your cluster ID
: "${TOPIC:=batched_scans}" 
: "${DATA_DIR:=/tmp/datagen/$TOPIC}"
: "${PREFIX:=$TOPIC}"
: "${SLEEP:=60}" # Time to sleep in between batches

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

# Upload files and archive files until told to stop
while [ `grep -c "^STOP" $STATUS_FILE` -eq 0 ] 
do
  # Upload files generated to Kafka and archive
  for staged_file in ${DATA_DIR}/stage/*.json
  do
    confluent kafka topic produce $TOPIC --cluster $CLUSTER_ID < $staged_file
    if [ $? -ne 0 ]; then
      log_message "ERR_" $THREAD "Exiting due to upload error" `date`
      log_message "STOP" `date`
      exit;
    fi
    log_message "SENT" $staged_file `date` 
    mv $staged_file $DATA_DIR/processed
  done
  sleep $SLEEP
done
log_message "EXIT" $THREAD `date`
