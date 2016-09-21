#/bin/sh

SLAVES="slave1 slave2 slave3 slave4"
for slave in $SLAVES; do
scp -P 2518 extlib/* ${slave}:~/Desktop/Stream/apache-storm-1.0.2/extlib
done
