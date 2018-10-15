#! /bin/bash


script=$1

echo script $script

python $script create publish

while true
do
    now=`date +%H:%M`
    for runtime in "$@"
    do
        if [ $now == $runtime ]
        then
            python $script update publish        
        fi
    done

    if [ `date +%M` == "00" ]
    then
        echo routing output per hour at `date +%Y-%m-%dT%H:%M:%S`
    fi

    sleep 60
done



