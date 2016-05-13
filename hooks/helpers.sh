#!/usr/bin/env bash

is_centos(){
    if [ -f /etc/redhat-release ];
      then
        return 0
      else
        return 1
    fi
}

is_systemd(){
    init=$(stat /proc/1/exe)
    if echo ${init} | grep -q "systemd"
    then
        return 0
    else
        return 1
    fi
}