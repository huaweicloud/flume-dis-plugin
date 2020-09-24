#!/bin/bash -l

KEY=$1

read -s -t 20 -p "Please enter password:" PASS

if [ $? -ne 0 ]; then
    echo ""
    echo "Failed to read password, exit."
    exit 1
fi

echo ""

if [ -z "${PASS}" ]; then
    echo "Get empty password."
    exit 1
fi


echo "Please wait..."

result=`java -cp "libext/*" com.huaweicloud.dis.adapter.flume.source.utils.EncryptTool "${PASS}" ${KEY}`

if [ $? -ne 0 ]; then
    echo "Failed to encrypt."
    exit 1
fi

echo "Encrypt result: ${result}"
