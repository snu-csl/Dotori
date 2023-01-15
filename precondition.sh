#!/bin/bash

DEV="/dev/nvme0n1"
PERCENT_FILL=0.80
DRIVE_CAPACITY=960197124096
KEY_SIZE=16
VALUE_SIZE=131072
GOAL=2147484 # MB
N_PAIRS=$(( $DRIVE_CAPACITY / ($KEY_SIZE + $VALUE_SIZE)))
N_PAIRS=$(echo " $N_PAIRS*$PERCENT_FILL / 1" | bc)
MB=$(( $N_PAIRS * ($KEY_SIZE + $VALUE_SIZE) ))
MB=$(( $MB >> 20 ))
TIMES=$(echo "scale=2; $GOAL / $MB" | bc)
FINAL=$(echo $TIMES | cut -d "." -f 2) # Assumes X.XX...
FINAL=$(( $FINAL * $N_PAIRS ))
FINAL=$(echo "$FINAL / 100" | bc)

function precondition {
    echo "Formatting ${DEV}..."
    nvme format -s 1 $DEV
    echo "Starting precondition"
    KVSSD/PDK/core/build/sample_code_async -d $DEV -k $KEY_SIZE -v $VALUE_SIZE -n $N_PAIRS -o 1 -r 0
    KVSSD/PDK/core/build/sample_code_async -d $DEV -k $KEY_SIZE -v $VALUE_SIZE -n $N_PAIRS -o 1 -r 1
    KVSSD/PDK/core/build/sample_code_async -d $DEV -k $KEY_SIZE -v $VALUE_SIZE -n $N_PAIRS -o 1 -r 1
}

precondition
