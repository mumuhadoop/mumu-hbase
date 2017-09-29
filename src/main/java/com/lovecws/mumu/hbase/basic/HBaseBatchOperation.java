package com.lovecws.mumu.hbase.basic;

import com.lovecws.mumu.hbase.HBaseConfiguration;
import com.lovecws.mumu.hbase.util.HBaseResultUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: hbase批量操作
 * @date 2017-09-27 13:27
 */
public class HBaseBatchOperation {

    private static final Logger log = Logger.getLogger(HBaseBatchOperation.class);

    /**
     * 批量添加数据
     *
     * @param tableName    表名称
     * @param rowKey       行健
     * @param columnFamily 列族
     * @param qualifier    列限定符
     * @param value        列值
     */
    public void putBatch(String tableName, String rowKey, String columnFamily, String qualifier, String value) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        List<Row> rows = new ArrayList<Row>();
        for (int i = 0; i < 100; i++) {
            Put put = new Put(Bytes.toBytes(rowKey + i));
            for (int j = 0; j < 10; j++) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier + j), Bytes.toBytes(value + j));
            }
            rows.add(put);
        }
        Result[] objs = new Result[rows.size()];
        try {
            table.batch(rows, objs);
            HBaseResultUtil.print(objs);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量获取
     *
     * @param tableName 表名
     * @param rowkeys   行健数组
     */
    public void getBatch(String tableName, String... rowkeys) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        List<Row> rows = new ArrayList<Row>();
        for (String rowkey : rowkeys) {
            rows.add(new Get(Bytes.toBytes(rowkey)));
        }
        try {
            Result[] objs = new Result[rows.size()];
            table.batch(rows, objs);
            HBaseResultUtil.print(objs);
        } catch (IOException | InterruptedException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 批量删除
     *
     * @param tableName 表名
     * @param rowkeys   行健数组
     */
    public void deleteBatch(String tableName, String... rowkeys) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        List<Row> rows = new ArrayList<Row>();
        for (String rowkey : rowkeys) {
            rows.add(new Delete(Bytes.toBytes(rowkey)));
        }
        try {
            Result[] objs = new Result[rows.size()];
            table.batch(rows, objs);
            HBaseResultUtil.print(objs);
        } catch (IOException | InterruptedException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
        }
    }
}
