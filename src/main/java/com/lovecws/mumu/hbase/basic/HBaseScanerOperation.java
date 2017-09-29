package com.lovecws.mumu.hbase.basic;

import com.lovecws.mumu.hbase.HBaseConfiguration;
import com.lovecws.mumu.hbase.util.HBaseResultUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: hbase扫描器
 * @date 2017-09-27 16:15
 */
public class HBaseScanerOperation {

    private static final Logger log = Logger.getLogger(HBaseScanerOperation.class);

    /**
     * hbase扫描器 一次获取多条记录
     *
     * @param tableName 表名
     * @param startRow  开始行健
     * @param count     一次获取的数量
     */
    public void scanner(String tableName, String startRow, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        ResultScanner scanner = null;
        try {
            Scan scan = new Scan();
            scan.setCaching(10);//缓存10行 之后在发送请求
            scan.setBatch(10);//缓存10列 每次next返回多少列
            if (startRow != null) {
                scan = new Scan(Bytes.toBytes(startRow));
            }
            scan.addFamily(Bytes.toBytes("baby"));//选择列族
            //scan.addColumn(Bytes.toBytes(""), Bytes.toBytes(""));//选择列
            scanner = table.getScanner(scan);
            //一次读取多条数据
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            log.error(e);
        } finally {
            hBaseConfiguration.close();
            scanner.close();
        }
    }
}
