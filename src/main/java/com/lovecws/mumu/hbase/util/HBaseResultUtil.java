package com.lovecws.mumu.hbase.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-09-27 14:53
 */
public class HBaseResultUtil {

    private static final Logger log = Logger.getLogger(HBaseResultUtil.class);

    /**
     * 格式化输出
     *
     * @param results
     */
    public static void print(Result... results) {
        log.info("查询结果:");
        for (Result result : results) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                log.info("rowkey:" + new String(CellUtil.cloneRow(cell)) + ","
                        + "columnFamily:" + new String(CellUtil.cloneFamily(cell)) + ","
                        + "qualifier:" + new String(CellUtil.cloneQualifier(cell)) + ","
                        + "Timetamp:" + cell.getTimestamp() + ","
                        + "value:" + new String(CellUtil.cloneValue(cell)) + ",");
            }
        }
    }
}
