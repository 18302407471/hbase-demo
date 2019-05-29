package com.hldu.hbase.dao;

public interface OperationDao {

    // 增加数据接口
    public void putData(String rowKey,String table);

    // 删除数据接口
    public void deleteData(String rowKey,String table);

    // 修改数据解耦
    public void updateData(String rowKey,String table);

    // 查看数据接口
    public void getData(String rowKey,String table);

    // 批量导入数据
    public void putAllData(String rowKey,String table);

    // 扫描表接口
    public void scanData(String startRow, String stopRow,String table);

    // 创建表
    public void createTable(String tableName);

    // 删除表
    public void deleteTable(String tableName);

    //创建列族
    public void addFamily(String tableName,String familyName);
}
