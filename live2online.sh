#!/bin/bash
if [[ $# != 1 ]];then
    echo "Usage:$0 sqlFile" >&2
    exit -1
fi
sqlFile="$1"

sed -Ei "s/connector\.type/connector/
/^.*'connector\.version.*$/d
/connector\.topic/ { s/connector\.topic/topic/; s/,([A-Z])/;\1/g; s/(.*);([a-zA-Z0-9_]+_his)(.*);\s*$/\1\3,/; s/$/\n\t'properties.allow.auto.create.topics' = 'false',\n\t'scan.topic-partition-discovery.interval' = '60s',/ }
#s/'connector\.startup-mode' = 'earliest-offset'/'scan\.startup\.mode' = 'latest-offset'/
/^.*'connector\.properties\.zookeeper\.connect.*$/d
/update-mode/d
s/connector\.properties\.bootstrap\.servers/properties\.bootstrap\.servers/
s/.*connector\.properties\.group\.id'\s*=\s*'(credit_online:)?([^']+)'.*$/\t'properties\.group\.id' = 'paredose_live_id:\2',/
s/format\.type/format/
s/credit_online:/paredose_live_id:/
s/shopee-hbase/hbase-2.2/
/^.*'connector\.model.*$/d
s/connector\.table-name/table-name/
s/connector\.zookeeper\.quorum/zookeeper\.quorum/
s/connector\.zookeeper\.znode\.parent/zookeeper\.znode\.parent/
s/connector\.write\.buffer-flush\.max-size/sink\.buffer-flush\.max-size/
s/connector\.write\.buffer-flush\.max-rows/sink\.buffer-flush\.max-rows/
s/connector\.write\.buffer-flush\.interval/sink\.buffer-flush\.interval/

#s/get_json_object\s*\(\s*from_Base64\s*\(\s*(Req\.)?Params\s*\)\s*,\s*'\\\$([^']*)'\s*\)/ReqParams\2/Ig
#
#s/(\s+)((.\.)?ReqParams\.(CommonHeader\.)?extension_process_data\.tongdun_deviec_info\.tongdun_df)/\1TRIM\(REPLACE\(\2,'_','-'\)\)/
#s/(\s+)((.\.)?ReqParams\.CommonHeader\.device_fingerprint)/\1TRIM\(REPLACE\(\2,'_','-'\)\)/
#s/(\s+)((.\.)?ReqParams\.DeviceFingerprint)/\1TRIM\(REPLACE\(\2,'_','-'\)\)/
#s/(TRIM\()((.\.)?ReqParams\.(CommonHeader\.)?extension_process_data\.tongdun_deviec_info\.tongdun_df)/TRIM\(REPLACE\(\2,'_','-'\)/
#s/(TRIM\()((.\.)?ReqParams\.CommonHeader\.device_fingerprint)/TRIM\(REPLACE\(\2,'_','-'\)/
#s/(TRIM\()((.\.)?ReqParams\.DeviceFingerprint)/TRIM\(REPLACE\(\2,'_','-'\)/
" $sqlFile
#sed -Ei '/./{H;$!d}; x; /shopee-hbase/ s/^/--/gM; /insert\s+into/I {p;s/^/--/gM; }' $sqlFile
sed -i ' :b; N; s/^\s*$//; tb ' $sqlFile
