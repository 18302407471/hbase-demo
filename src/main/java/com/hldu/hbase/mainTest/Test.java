package com.hldu.hbase.mainTest;

import com.hldu.hbase.impl.OperationDaoImpl;

public class Test {
    public static void main(String[] args) {
        OperationDaoImpl od = new OperationDaoImpl();
        String tableName = "testApi";
        String rowKey = "rk";
//        od.createTable(tableName);
        od.putData(rowKey,tableName);
//        od.getData(rowKey,tableName);
//        od.updateData(rowKey,tableName);
//        od.deleteData(rowKey,tableName);
//        od.putAllData(rowKey,tableName);
//        od.scanData("rk1", "rk30",tableName);
//        od.deleteTable(tableName);
//        od.addFamily(tableName,"family001");
    }
}
