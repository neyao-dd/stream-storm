#!/bin/bash - 
#===============================================================================
#
#          FILE: runDRPC.sh
# 
#         USAGE: ./runDRPC.sh 
# 
#   DESCRIPTION: 
# 
#       OPTIONS: ---
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: YOUR NAME (), 
#  ORGANIZATION: 
#       CREATED: 08/31/2016 11:41
#      REVISION:  ---
#===============================================================================

set -o nounset                              # Treat unset variables as an error

nohup bin/storm drpc >> drpc.log 2>&1 &

