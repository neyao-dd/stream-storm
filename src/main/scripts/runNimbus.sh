#!/bin/bash - 
#===============================================================================
#
#          FILE: runNimbus.sh
# 
#         USAGE: ./runNimbus.sh 
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

nohup bin/storm nimbus >> nimbus.log 2>&1 &

