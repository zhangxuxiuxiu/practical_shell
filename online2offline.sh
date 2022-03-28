#!/bin/bash

if [[ $# != 1 ]]; then
    echo "Usage: $0 sqlFile" >&2
    exit -1
fi
sql_file="$1"
metric_name=$(sed -E 's/(.*\/)?(.+)\.sql/\2/' <<<"$sql_file")

sed -i".online" -E "
# 1) split into paragraph
:f
/;\s*$/!{N; bf}

# 2) expand kafka connector to multiple filesystem connectors, one filesystem connector per topic
/'connector'\s*=\s*'kafka'/ {
# 2.1) one filesystem connector copy for each topic
:a
H
s/(^\s*'topic' = ')[^;]+;(.+$)/\1\2/M
ta
z
x
# 2.2) delete non-first topics
s/(^\s*'topic' = '[^;]+);.*$/\1',/gM

# 2.3) convert topics to hdfs urls
# parquet data format from following hdfs dir
# hdfs:///data/streaming-file/hbase/app_id=kredit/scene_id=10002
# hdfs:///data/streaming-file/kafka/ID_live_credit_risk_gateway_req_rsp/app_id=kredit/scene_id=10001
# hdfs:///data/streaming-file/kafka/ID_live_credit_risk_gateway_req_rsp_202108_no-kafka-meta/app_id=kredit/scene_id=10002
# json data format hdfs dir: hdfs://data/streaming-file/json/ID_live_credit_risk_gateway_req_rsp/app_id=kredit/scene_id=10002
s/'ID_live_credit_risk_gateway_req_rsp_([a-z]+)_([0-9]+)/'hdfs:\/\/\/data\/streaming-file\/json\/ID_live_credit_risk_gateway_req_rsp\/app_id=\1\/scene_id=\2/g
}

# 3) append hbase connector & insert after \"create view xxx_view\"
s/create\s+view\s+([a-z_]+)_unq\s+as.*$/&\n\ncreate table \1_hb(\n\trk STRING,\n\tcf ROW<\1_unq String>\n\) with \(\n\t'connector' = 'hbase-ts',\n\t'table-name' = 'paredose_live_id:${metric_name}',\n\t'zookeeper.quorum' = 'zk-dci-001:2181,zk-dci-002:2181,zk-dci-003:2181',\n\t'sink.buffer-flush.max-size' ='1mb',\n\t'sink.buffer-flush.max-rows' ='1000',\n\t'sink.buffer-flush.interval' ='500',\n\t'null-string-literal' = ''\n\);\n\ninsert into \1_hb\nselect rk,row(val)\nfrom \(\n\tselect rk, bitmap_of\(val\) as val\n\tfrom \(\n\t\tselect concat\(key, '_', cast\(ts\/60*60 as String\)\) as rk, val from \1_unq where ts<1637313098\n\t\t\)\n\tgroup by rk\n\t\);\n\n/
#" "$sql_file"

# 4) extract schema & topics, convert kafka connector to filesystem connector with table name $appid_$sceneid
sed -i -E "
:f
/;\s*$/!{N; bf}
/'connector'\s*=\s*'kafka'/ s/\s*create\s+table\s+[^ \t]+\s+\((.*)\s*\)\s*with\s*\(.*'topic'\s*=\s*'(.*app_id=([a-z]+)\/scene_id=([0-9]+))',.*/create table \3_\4 (\1 ) with (\n\t'connector' = 'filesystem',\n\t'path' = '\2',\n\t'format' = 'json'\n\t);\n/I
" "$sql_file"

# 5) squeeze empty lines
sed -i ':b; N; s/^\n*$//; tb' "$sql_file"
