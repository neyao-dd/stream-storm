#!/bin/sh

bin/storm jar extlib/StreamStorm-1.0-SNAPSHOT.jar org.apache.storm.flux.Flux --remote conf/ESTopology.yaml --filter  conf/deepdata.properties
