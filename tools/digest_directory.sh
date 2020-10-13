#!/usr/bin/env bash
#read $1
for var in "$@"
do
    echo $var
    for f in $var
    do
        for f2 in $f/week*
        do
            echo "$f2"
            ex -s -c '1i|# CONTEXT-DATABASE: lapis ' -c x $f2
            ex -s -c '1i|# DML ' -c x $f2
            influx -import -path=$f2
        done
    done
done