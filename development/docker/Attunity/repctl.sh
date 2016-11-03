#!/bin/bash

#####################################################
#
# Invocation of repctl from local directory
#
#####################################################

AR_PATH=/opt/attunity/replicate
export LD_LIBRARY_PATH="$AR_PATH/lib:"

$AR_PATH/bin/repctl $1 $2 $3 $4

