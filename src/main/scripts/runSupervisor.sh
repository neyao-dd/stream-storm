#!/bin/bash - 
#===============================================================================
#
#          FILE: runSupervisor.sh
# 
#         USAGE: ./runSupervisor.sh 
# 
#   DESCRIPTION: 
# 
#       OPTIONS: ---
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: YOUR NAME (), 
#  ORGANIZATION: 
#       CREATED: 08/31/2016 11:42
#      REVISION:  ---
#===============================================================================

set -o nounset                              # Treat unset variables as an error

nohup bin/storm supervisor >> supervisor.log 2>&1 &

