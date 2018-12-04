#! /bin/bash

export JQM1=conf-constant.yml

python constant.py create find
python jqdata.py publish      
python constant.py publish

while true
do
    now=`date +%H:%M`
    for runtime in "$@"
    do
        if [ $now == $runtime ]
        then
            python constant.py create find
            python jqdata.py publish      
            python constant.py publish
        fi
    done

    if [ `date +%M` == "00" ]
    then
        echo routing output per hour at `date +%Y-%m-%dT%H:%M:%S`
    fi

    sleep 60
done
