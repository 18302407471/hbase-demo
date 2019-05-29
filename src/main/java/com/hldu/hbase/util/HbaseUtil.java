package com.hldu.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

public class HbaseUtil {
    // 获取HTable
    public static HTable getConf(String tableName) {
        // 创建俺新conf
        Configuration conf = HBaseConfiguration.create();
        HTable htbl = null;
        try {
            htbl = new HTable(conf, tableName);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return htbl;
    }

    // 关闭资源
    public static void closeHtable(HTable htbl) {
        try {
            htbl.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
