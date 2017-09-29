package com.lovecws.mumu.hbase.basic;

import com.lovecws.mumu.hbase.HBaseConfiguration;
import com.lovecws.mumu.hbase.util.HBaseResultUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: hbase基本操作
 * @date 2017-09-27 10:05
 */
public class HBaseRowOperation {

    private static final Logger log = Logger.getLogger(HBaseRowOperation.class);

    /**
     * 添加一列数据
     *
     * @param tableName    表名称
     * @param rowKey       行健
     * @param columnFamily 列族
     * @param qualifier    列限定符
     * @param value        列值
     */
    public void insertRow(String tableName, String rowKey, String columnFamily, String qualifier, String value) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        //创建行健
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), System.currentTimeMillis(), Bytes.toBytes(value));
        try {
            table.put(put);
            log.info("数据添加成功:" + put);
        } catch (IOException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 添加多列数据
     *
     * @param tableName 表名称
     * @param rowKey    行健
     * @param columns   列数据
     */
    public void insertMultiRow(String tableName, String rowKey, List<Map<String, String>> columns) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        //创建行健
        Put put = new Put(Bytes.toBytes(rowKey));
        //添加多列数据
        for (Map<String, String> columnMap : columns) {
            put.addColumn(Bytes.toBytes(columnMap.get("columnFamily")), Bytes.toBytes(columnMap.get("qualifier")), System.currentTimeMillis(), Bytes.toBytes(columnMap.get("value")));
        }
        try {
            table.put(put);
            log.info("数据添加成功:" + put);
        } catch (IOException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 批量添加多行
     *
     * @param tableName    表名称
     * @param rowKey       行健
     * @param columnFamily 列族
     * @param qualifier    列限定符
     * @param value        列值
     */
    public void insertBatchRow(String tableName, String rowKey, String columnFamily, String qualifier, String value) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        //10行
        List<Put> putList = new ArrayList<Put>();
        for (int i = 1; i < 10; i++) {
            Put put = new Put(Bytes.toBytes(rowKey + i));
            //每行包含5列
            for (int j = 1; j <= 5; j++) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier + j), Bytes.toBytes(value + j));
            }
            putList.add(put);
            log.info("数据添加成功:" + put);
        }
        try {
            table.put(putList);
        } catch (IOException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 检测旧值 并设置新值
     *
     * @param tableName    表名称
     * @param rowKey       行健
     * @param columnFamily 列族
     * @param qualifier    列限定符
     * @param oldValue     旧值
     * @param newValue     新值
     */
    public void checkAndPut(String tableName, String rowKey, String columnFamily, String qualifier, String oldValue, String newValue) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Put put = new Put(rowKey.getBytes());
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(newValue));
        try {
            boolean checkAndPut = table.checkAndPut(Bytes.toBytes(rowKey), Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(oldValue), put);
            log.info("数据更新:" + (checkAndPut ? "成功" : "失败"));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 获取行值
     *
     * @param tableName 表名称
     * @param rowKey    行健
     */
    public void getRowValue(String tableName, String rowKey) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            Result result = table.get(get);
            HBaseResultUtil.print(result);
        } catch (IOException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 获取列族值
     *
     * @param tableName    表名称
     * @param rowKey       行健
     * @param columnFamily 列族
     */
    public void getFamilyValue(String tableName, String rowKey, String columnFamily) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(Bytes.toBytes(columnFamily));
        try {
            Result result = table.get(get);
            HBaseResultUtil.print(result);
        } catch (IOException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 获取列值
     *
     * @param tableName    表名称
     * @param rowKey       行健
     * @param columnFamily 列族
     * @param qualifier    列族标识符
     */
    public void getColumnValue(String tableName, String rowKey, String columnFamily, String qualifier) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
        try {
            get.setMaxVersions(3);//获取三个版本值
            Result result = table.get(get);
            HBaseResultUtil.print(result);
        } catch (IOException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 批量获取行值
     *
     * @param tableName 表名
     * @param rowkeys   行健数组
     */
    public void getBatchRow(String tableName, String... rowkeys) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        List<Get> getList = new ArrayList<Get>();
        for (String rowkey : rowkeys) {
            getList.add(new Get(Bytes.toBytes(rowkey)));
        }
        try {
            Result[] results = table.get(getList);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 检测行、列族、列是否存在
     *
     * @param tableName 表名
     * @param rowKey    行健数组
     */
    public void exists(String tableName, String rowKey, String columnFamily, String qualifier) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Get get = new Get(Bytes.toBytes(rowKey));//检测行是否存在
        //get.addFamily(Bytes.toBytes(columnFamily));//检测列族是否存在
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));//检测列是否存在
        try {
            boolean exists = table.exists(get);
            log.info("数据" + (exists ? "存在" : "不存在"));
        } catch (IOException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
        }
    }


    /**
     * 删除一行数据
     *
     * @param tableName 表名称
     * @param rowKey    行健
     */
    public void deleteRow(String tableName, String rowKey) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        try {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            log.info("删除行成功");
        } catch (IOException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 删除一列数据
     *
     * @param tableName    表名称
     * @param rowKey       行健
     * @param columnFamily 列族
     * @param qualifier    列族标识符
     */
    public void deleteColumn(String tableName, String rowKey, String columnFamily, String qualifier) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        try {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
            table.delete(delete);
            log.info("删除列成功");
        } catch (IOException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 删除列族
     *
     * @param tableName    表名称
     * @param rowKey       行健
     * @param columnFamily 列族
     */
    public void deleteFamily(String tableName, String rowKey, String columnFamily) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        try {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addFamily(Bytes.toBytes(columnFamily));
            table.delete(delete);
            log.info("删除列族成功");
        } catch (IOException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 检测并删除一列值
     *
     * @param tableName    表名称
     * @param rowKey       行健
     * @param columnFamily 列族
     * @param qualifier    列限定符
     * @param value        列值
     */
    public void checkAndDelete(String tableName, String rowKey, String columnFamily, String qualifier, String value) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        try {
            Delete delete = new Delete(rowKey.getBytes()); //删除一行
            delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier)); //删除一列
            //delete.addFamily(Bytes.toBytes(columnFamily)); //删除列族
            boolean checkAndDelete = table.checkAndDelete(Bytes.toBytes(rowKey), Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value), delete);
            log.info("删除:" + (checkAndDelete ? "成功" : "失败"));
        } catch (IOException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 批量删除多行
     *
     * @param tableName 表名称
     * @param rowKeys   行健集合
     */
    public void deleteBatch(String tableName, String... rowKeys) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        try {
            List<Delete> deleteList = new ArrayList<Delete>();
            for (String rowkey : rowKeys) {
                Delete delete = new Delete(rowkey.getBytes()); //删除一行
                deleteList.add(delete);
            }
            table.delete(deleteList);
            log.info("批量删除成功");
        } catch (IOException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 行追加列
     *
     * @param tableName    表名
     * @param rowKey       行健
     * @param columnFamily 列族
     * @param qualifier    列限定符
     * @param value        列值
     */
    public void append(String tableName, String rowKey, String columnFamily, String qualifier, String value) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Append append = new Append(Bytes.toBytes(rowKey));
        append.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        //append.add(new KeyValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value)));
        try {
            Result result = table.append(append);
            HBaseResultUtil.print(result);
        } catch (IOException e) {
            log.error(e);
            System.exit(-1);
        } finally {
            hBaseConfiguration.close();
        }
    }

    /**
     * 列值自增
     *
     * @param tableName    表名
     * @param rowKey       行健
     * @param columnFamily 列族
     * @param qualifier    列限定符
     * @param value        列值
     */
    public void increment(String tableName, String rowKey, String columnFamily, String qualifier, long value) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        try {
            //Increment increment = new Increment(Bytes.toBytes(rowKey));
            //increment.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), value);
            //table.increment(increment);
            long incrementColumnValue = table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), value);
            System.out.println("自增:" + incrementColumnValue);
        } catch (IOException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
        }
    }
}
