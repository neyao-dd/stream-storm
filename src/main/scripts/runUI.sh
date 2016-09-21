#!/bin/bash - 
#===============================================================================
#
#          FILE: runUI.sh
# 
#         USAGE: ./runUI.sh 
# 
#   DESCRIPTION: 
# 
#       OPTIONS: ---
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: YOUR NAME (), 
#  ORGANIZATION: 
#       CREATED: 08/31/2016 11:43
#      REVISION:  ---
#===============================================================================

set -o nounset                              # Treat unset variables as an error

nohup bin/storm ui >> ui.log 2>&1 &

