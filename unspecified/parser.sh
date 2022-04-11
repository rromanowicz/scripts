#/bin/bash

INPUT=input_file.txt
LOGFILE=logfile.txt
PROCESSED_DIR=oldfiles
DIFF_DIR=diff
FILE_RETENTION_DAYS=6
AWS_DYNAMODB_URL=http://localhost:8000 
TBL_NAME=TestTbl
AWS_CLI=/home/ex/Downloads/aws/dist/aws
OLDIFS=$IFS
IFS=';'
DATE_FORMAT="%Y-%m-%d"
DATETIME_FORMAT="%Y-%m-%d %T"


if [[ $* == *--debug* ]]
then
	rm $LOGFILE
	rm ./$PROCESSED_DIR/$(date +$DATE_FORMAT)_$INPUT
	rm ./$DIFF_DIR/$(date +$DATE_FORMAT)_Diff.txt
fi


#Call AWS CLI to put item in DynamoDB
function aws_put_item() {
		$AWS_CLI dynamodb put-item \
	    --endpoint-url $AWS_DYNAMODB_URL \
		  --table-name $TBL_NAME \
	    --item '{
	        "Idx": {"S": "'$1'"},
	        "Val": {"S": "'$2'"} 
		  }' \
		  --return-consumed-capacity INDEXES &
}

function duplicate_key() {
	printf "%s - Duplicate key found [%s]! Existing Value: [%s], Duplicate Value: [%s]\n" $(date +"$DATETIME_FORMAT") $1 $2 $3 >> $LOGFILE
}

function pre_processing() {
  #Check if input file exists.
  [ ! -f $INPUT ] && { echo "$INPUT file not found."; exit 1; }
  #Check if file with current date already exists in $PROCESSED_DIR
  [ -f ./$PROCESSED_DIR/$(date +$DATE_FORMAT)_$INPUT ] && { echo "File already processed."; exit 1; }

  connection_check=$(/home/ex/Downloads/aws/dist/aws dynamodb list-tables --endpoint-url $AWS_DYNAMODB_URL)
  if [[ ! $connection_check ]];
  then
  	echo "$(date +"$DATETIME_FORMAT") - Failed to connect to $AWS_DYNAMODB_URL. Exiting script." >> $LOGFILE
  	exit 1
  else
  	if [[ ! $connection_check =~ $TBL_NAME ]]
  	then
		  $AWS_CLI dynamodb create-table \
        --endpoint-url $AWS_DYNAMODB_URL \
        --table-name $TBL_NAME \
        --attribute-definitions AttributeName=Idx,AttributeType=S \
        --key-schema AttributeName=Idx,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1
	  fi
  fi
}

function process_file() {
  declare -A inputArray

  #Read $INPUT file, check for duplicates and create input array
  while read key value
  do
  	if [[ -n ${inputArray[$key]} || -z ${inputArray[$key]-foo} ]]; then
		#Log duplicate entries
  		duplicate_key $key ${inputArray[$key]} $value
	  else
		inputArray[$key]="$value"
  	fi
  done <$INPUT
  IFS=$OLDIFS

  #Process deduplicated data and save to DB
  for i in "${!inputArray[@]}"
  do
    echo "key: $i, value: ${inputArray[$i]}"
    aws_put_item $i ${inputArray[$i]}
  done

  echo "$(date +"$DATETIME_FORMAT") - Processed ${#inputArray[@]} records." >> $LOGFILE
}

function archive_file() {
  cp $INPUT ./$PROCESSED_DIR/$(date +$DATE_FORMAT)_$INPUT
  echo "$(date +"$DATETIME_FORMAT") - ./$PROCESSED_DIR/$(date +$DATE_FORMAT)_$INPUT file created." >> $LOGFILE
}

function remove_old_files() {
	echo "$(date +"$DATETIME_FORMAT") - Removing files older then $(($FILE_RETENTION_DAYS+1)) days." >> $LOGFILE

	ls ./$PROCESSED_DIR | xargs -n 1 basename | while read -r line; do
	if [[ $(date -d ${line:0:10} "+%Y-%m-%d") < $(date -d "$FILE_RETENTION_DAYS days ago" "+%Y-%m-%d") ]]
		then
			rm ./$PROCESSED_DIR/$line
			echo "$(date +"$DATETIME_FORMAT") - File $line removed." >> $LOGFILE
		fi
	done
}

#Generate diff file with new and removed entries.
function file_diff() {
	DIFF_FILE=./$DIFF_DIR/$(date +$DATE_FORMAT)_Diff.txt
	YESTERDAY_FILE=./$PROCESSED_DIR/$(date +$DATE_FORMAT -d "1 day ago")_$INPUT

	printf "New entries:\n" >> $DIFF_FILE
	comm -23 <(sort $INPUT) <(sort $YESTERDAY_FILE) >> $DIFF_FILE
	printf "\nRemoved entries:\n" >> $DIFF_FILE
	comm -13 <(sort $INPUT) <(sort $YESTERDAY_FILE)>> $DIFF_FILE
	echo "$(date +"$DATETIME_FORMAT") - $DIFF_FILE created." >> $LOGFILE
	echo "$(date +"$DATETIME_FORMAT") - $(cat $DIFF_FILE)" >> $LOGFILE
}

pre_processing
process_file
archive_file
remove_old_files
file_diff

exit 0
