#!/bin/sh

DIR=$(cd `dirname $0`; pwd)
SLAVES="slave1 slave2 slave3 slave4"

for slave in $SLAVES; do
ssh -T -p 2518 hadoop@${slave} << EOT
hostname
cd $DIR
rm -rf logs/workers-artifacts/*
EOT
done
