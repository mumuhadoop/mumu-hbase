package com.lovecws.mumu.hbase.basic;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: hbase基本curd
 * @date 2017-09-27 11:25
 */
public class HBaseRowOperationTest {

    private HBaseRowOperation hBaseRowOperation = new HBaseRowOperation();
    private HBaseTableOperation hBaseTableOperation = new HBaseTableOperation();

    @Test
    public void insertRow() {
        //先创建表
        hBaseTableOperation.createTable("babymm", "baby", "mm");
        //插入一行记录
        hBaseRowOperation.insertRow("babymm", "row1", "baby", "b1", "my lover baby mumu");
    }

    @Test
    public void insertMultiRow() {
        List<Map<String, String>> columns = new ArrayList<Map<String, String>>();
        for (int i = 1; i <= 10; i++) {
            Map<String, String> columnMap = new HashMap<String, String>();
            columnMap.put("columnFamily", "baby");
            columnMap.put("qualifier", "b" + i);
            columnMap.put("value", "value" + i);
            columns.add(columnMap);
        }
        for (int i = 1; i <= 5; i++) {
            Map<String, String> columnMap = new HashMap<String, String>();
            columnMap.put("columnFamily", "mm");
            columnMap.put("qualifier", "m" + i);
            columnMap.put("value", "mm" + i);
            columns.add(columnMap);
        }
        hBaseRowOperation.insertMultiRow("babymm", "row2", columns);
    }

    @Test
    public void insertBatchRow() {
        hBaseRowOperation.insertBatchRow("babymm", "row3", "baby", "b1", "mumu");
    }

    @Test
    public void checkAndPut() {
        hBaseRowOperation.checkAndPut("babymm", "row2", "baby", "b1", "value1", "value11111");
    }

    @Test
    public void getRowValue() {
        hBaseRowOperation.getRowValue("babymm", "row2");
    }

    @Test
    public void getFamilyValue() {
        hBaseRowOperation.getFamilyValue("babymm", "row2", "mm");
    }

    @Test
    public void getColumnValue() {
        hBaseRowOperation.getColumnValue("babymm", "row2", "baby", "b1");
    }

    @Test
    public void getBatchRow() {
        hBaseRowOperation.getBatchRow("babymm", "row2", "row1", "row31");
    }

    @Test
    public void exists() {
        hBaseRowOperation.exists("babymm", "row2", "baby", "b1");
    }

    @Test
    public void deleteColumn() {
        hBaseRowOperation.deleteColumn("babymm", "row2", "mm", "m1");
    }

    @Test
    public void deleteRow() {
        hBaseRowOperation.deleteRow("babymm", "row2");
    }

    @Test
    public void deleteFamily() {
        hBaseRowOperation.deleteFamily("babymm", "row2", "mm");
    }

    @Test
    public void checkAndDelete() {
        hBaseRowOperation.checkAndDelete("babymm", "row2", "baby", "b10", "value10");
    }

    @Test
    public void deleteBatch() {
        hBaseRowOperation.deleteBatch("babymm", "row39", "row38", "row37", "row36");
    }

    @Test
    public void append() {
        hBaseRowOperation.append("babymm", "row2", "baby", "b11", "3");
    }

    @Test
    public void increment() {
        hBaseRowOperation.increment("babymm", "row2", "baby", "b11", 1);
    }
}
