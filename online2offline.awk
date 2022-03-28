#!/usr/bin/awk -f

BEGIN {
    FS="\n"
    RS=""
    IGNORECASE = 1
    metric_name = ""
    if(match(ARGV[1],"(.*\\/)?(.+)\\.sql", name_arr) > 0 ){
        metric_name = name_arr[2]
    } else {
        print ARGV[1] " is not valid sql file" > "/dev/stderr"
        exit -1
    }
}

{
    if( match($0, "\\s*create\\s+table\\s+([^ \t]+)\\s*\\((.+)\\s+\\)\\s*with\\s*\\(\\s+(--.*)?\\s*'connector'\\s*=\\s*'kafka',\\s+'topic'\\s*=\\s*'([^']+)'", arr) > 0 ) {
        kafka_table_name = arr[1]
        kafka_schema = arr[2]
        kafka_topics = arr[4]

        split(kafka_topics, topics_arr, ";")
#        printf("matched kafka_topics:%s", kafka_topics)
        for (topic_idx  in topics_arr) {
            if ( match(topics_arr[topic_idx], ".*req_rsp_([a-z]+)_([0-9]+)", app_scene) > 0 ) {
                app_id = app_scene[1]
                scene_id = app_scene[2]
                view_name = app_id "_" scene_id
                view_names[topic_idx] = view_name
                hdfs_url = "hdfs:///data/streaming-file/json/ID_live_credit_risk_gateway_req_rsp/app_id=" app_id "/scene_id=" scene_id
                print "create table " view_name " (" kafka_schema "\n) with (\n\t'connector' = 'filesystem',\n\t'path' = '" hdfs_url "',\n\t'format' = 'json'\n\t);\n"
            } else if( topics_arr[topic_idx]  ~ "_his$") {
                print "ignore history topic:" topics_arr[topic_idx] > "/dev/stderr"
            } else {
                print "invalid kafka topic:" topics_arr[topic_idx] > "/dev/stderr"
            }
        }

        print "create view " kafka_table_name " as \n("
        for(view_idx in view_names) {
            if(view_idx !=1) print "union all"
            print "( select * from " view_names[view_idx] ")"
        }
        print ");\n"
        delete view_names

         next
    }

    if ( match($0, "create\\s+view\\s+([^ \t]+)_unq\\s+as", hbase_arr) > 0) {
        print $0 "\n"
        hbase_view_name = hbase_arr[1]
        print "create table " hbase_view_name "_offline(\n\trk STRING,\n\tcf ROW<" hbase_view_name "_unq String>\n) with (\n\t'connector' = 'hbase-ts',\n\t'table-name' = 'paredose_live_id:" metric_name "',\n\t'zookeeper.quorum' = 'zk-dci-001:2181,zk-dci-002:2181,zk-dci-003:2181',\n\t'sink.buffer-flush.max-size' ='1mb',\n\t'sink.buffer-flush.max-rows' ='1000',\n\t'sink.buffer-flush.interval' ='500',\n\t'null-string-literal' = ''\n);\n\ninsert into " hbase_view_name "_offline\nselect rk,row(val)\nfrom (\n\tselect rk, bitmap_of(val) as val\n\tfrom (\n\t\tselect concat(key, '_', cast(ts/60*60 as String)) as rk, val from " hbase_view_name "_unq where ts>=1629651600 and ts<1637600400\n\t\t)\n\tgroup by rk\n\t);\n\n"
        next
    }

    if( $0 !~ "^\\s*$"){
        print $0 "\n"
    }
}
