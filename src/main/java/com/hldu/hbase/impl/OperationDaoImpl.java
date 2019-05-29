package com.hldu.hbase.impl;

import com.hldu.hbase.dao.OperationDao;
import com.hldu.hbase.util.HbaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OperationDaoImpl implements OperationDao {
    public void putData(String rowKey,String table) {
        try {
            HTable htbl = HbaseUtil.getConf(table);
            // 将行键传入put
            Put put = new Put(Bytes.toBytes(rowKey));
            // 增加数据
            put.add(Bytes.toBytes("info"), Bytes.toBytes("num"), Bytes.toBytes("hadoop"+1));
            Map<String,String> map = new HashMap<String, String>();
            map.put("test1","2019052901");
            map.put("test2","2019052902");
            map.put("test3","2019052903");
            put.add("family001".getBytes(),"column1".getBytes(),new JSONObject(map).toString().getBytes());
            htbl.put(put);
            HbaseUtil.closeHtable(htbl);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteData(String rowKey,String table) {
        // 创建新的Conf

        try {
            HTable htbl = HbaseUtil.getConf(table);

            // 将行键传入delete
            Delete del = new Delete(Bytes.toBytes(rowKey));
            // 删除行
            del.deleteColumn(Bytes.toBytes("info"), Bytes.toBytes("num"));
            htbl.delete(del);
            HbaseUtil.closeHtable(htbl);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void updateData(String rowKey,String table) {
        try {
            HTable htbl = HbaseUtil.getConf(table);
            // 将行键传入put
            Put put = new Put(Bytes.toBytes(rowKey));
            // 增加数据
            put.add(Bytes.toBytes("info"), Bytes.toBytes("num"), Bytes.toBytes("hadoop"+3));
            htbl.put(put);
            HbaseUtil.closeHtable(htbl);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getData(String rowKey,String table) {
        try {
            HTable htbl = HbaseUtil.getConf(table);
            // 将行键传入get
            Get get = new Get(Bytes.toBytes(rowKey));
            // 添加查询条件
            get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("column1"));
            // 获取结果集合
            Result rs = htbl.get(get);
            Cell[] cells = rs.rawCells();
            // 循环遍历结果
            for (Cell cell : cells) {
                // 打印结果
                System.out.print(Bytes.toString(CellUtil.cloneFamily(cell)) + ":");
                System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell)) + "->");
                System.out.print(Bytes.toString(CellUtil.cloneValue(cell)));
            }
            System.out.println();
            HbaseUtil.closeHtable(htbl);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void putAllData(String rowKey,String table) {
        try {
            HTable htbl = HbaseUtil.getConf(table);
            List<Put> list = new ArrayList<Put>(10000);
            // 增加数据
            Put put = new Put(Bytes.toBytes(rowKey));
            for (long i = 1; i <= 100; i++) {
                put.add(Bytes.toBytes("info"), Bytes.toBytes("num"), Bytes.toBytes("hadoop" + i));
                list.add(put);
                //每到10万次导入一次数据
                if (i % 100000 == 0) {
                    htbl.put(list);
                    list = new ArrayList<Put>(10000);
                }
            }
            //数据如果不是10万整数倍，剩余数据循环结束一次导入
            htbl.put(list);
            HbaseUtil.closeHtable(htbl);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void scanData(String startRow, String stopRow,String table) {
        HTable htbl = null;
        ResultScanner rss = null;
        try {
            htbl = HbaseUtil.getConf(table);
            // 将行键传入scan
            Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
            // 添加查询条件
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("num"));
            // 获取结果集集合
            rss = htbl.getScanner(scan);
            // 遍历结果集集合
            for (Result rs : rss) {
                System.out.print(Bytes.toString(rs.getRow())+"\t");
                // 遍历结果集合
                Cell[] cells = rs.rawCells();
                for (Cell cell : cells) {
                    System.out.print(Bytes.toString(CellUtil.cloneFamily(cell)) + ":");
                    System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell)) + "->");
                    System.out.print(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            IOUtils.closeStream(rss);
            IOUtils.closeStream(htbl);
        }
    }

    public void createTable(String tableName) {
        Configuration conf = HBaseConfiguration.create();
        //conf.set("hbase.zookeeper.quorum", "master");
        try {
            HBaseAdmin hba = new HBaseAdmin(conf);
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor info = new HColumnDescriptor("info");
            info.setValue("num", "003");
            htd.addFamily(info);
            hba.createTable(htd);
            hba.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void deleteTable(String tableName) {
        Configuration conf = HBaseConfiguration.create();
        try {
            HBaseAdmin hba = new HBaseAdmin(conf);
            hba.disableTable(tableName);
            hba.deleteTable(tableName);
            hba.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void addFamily(String tableName,String familyName){
        Configuration conf = HBaseConfiguration.create();
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            HTableDescriptor htd = admin.getTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(familyName);
            htd.addFamily(hColumnDescriptor);
            admin.modifyTable(TableName.valueOf(tableName),htd);
            admin.enableTable(tableName);
            admin.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
