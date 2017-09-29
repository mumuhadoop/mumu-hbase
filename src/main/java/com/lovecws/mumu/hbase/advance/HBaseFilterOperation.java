package com.lovecws.mumu.hbase.advance;

import com.lovecws.mumu.hbase.HBaseConfiguration;
import com.lovecws.mumu.hbase.util.HBaseResultUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: hbase过滤器使用
 * @date 2017-09-27 16:58
 */
public class HBaseFilterOperation {

    private static final Logger log = Logger.getLogger(HBaseFilterOperation.class);

    /**
     * 使用行过滤器 选择大于rowKey的行
     *
     * @param tableName 表名
     * @param rowKey    行健
     * @param count     数量
     */
    public void rowFilter(String tableName, String rowKey, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();
        //使用行过滤器 选择大于 rowkey的行
        //scan.setFilter(new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes(rowKey))));//直接行健
        //scan.setFilter(new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new RegexStringComparator("row.*")));//正则表达式
        //scan.setFilter(new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new SubstringComparator("row")));//字符串包含
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryPrefixComparator("row".getBytes())));//字符串前缀
        scan.setCaching(10);
        scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 列族过滤器
     *
     * @param tableName 表名
     * @param rowFamily 列族
     * @param count     数量
     */
    public void familyFilter(String tableName, String rowFamily, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();
        //使用列族过滤器
        //scan.setFilter(new FamilyFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes(rowFamily))));//直接行健
        //scan.setFilter(new FamilyFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new RegexStringComparator("row.*")));//正则表达式
        //scan.setFilter(new FamilyFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new SubstringComparator("row")));//字符串包含
        scan.setFilter(new FamilyFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryPrefixComparator("mm".getBytes())));//字符串前缀
        scan.setCaching(10);
        scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 列限定符过滤器
     *
     * @param tableName  表名
     * @param columnName 列限定符
     * @param count      数量
     */
    public void qualifierFilter(String tableName, String columnName, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();
        //使用列族过滤器
        scan.setFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(columnName))));//直接行健
        //scan.setFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("row.*")));//正则表达式
        //scan.setFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("row")));//字符串包含
        //scan.setFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator("m".getBytes())));//字符串前缀
        scan.setCaching(10);
        scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 列限定符过滤器
     *
     * @param tableName   表名
     * @param columnValue 列值
     * @param count       数量
     */
    public void valueFilter(String tableName, String columnValue, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();
        //使用列族过滤器
        scan.setFilter(new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(columnValue))));//直接行健
        //scan.setFilter(new ValueFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("row.*")));//正则表达式
        //scan.setFilter(new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("row")));//字符串包含
        //scan.setFilter(new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator("mm".getBytes())));//字符串前缀
        scan.setCaching(10);
        scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 参考列过滤器(获取相同时间戳的列)
     *
     * @param tableName    表名
     * @param columnFamily 列族
     * @param qualifier    列限定符
     * @param columnValue  列值
     * @param count        数量
     */
    public void dependentColumnFilter(String tableName, String columnFamily, String qualifier, String columnValue, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes("")));
        scan.setCaching(10);
        scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 单列过滤器（比对一列值 如果相符 就获取整行数据）
     *
     * @param tableName    表名
     * @param columnFamily 列族
     * @param qualifier    列限定符
     * @param columnValue  列值
     * @param count        数量
     */
    public void singleColumnValueFilter(String tableName, String columnFamily, String qualifier, String columnValue, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(columnValue));
        singleColumnValueFilter.setFilterIfMissing(true);//当不存在这列的行 默认不过滤
        singleColumnValueFilter.setLatestVersionOnly(true);//获取最新版本
        scan.setFilter(singleColumnValueFilter);
        scan.setCaching(10);
        //scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 单列排除过滤器（返回的列 不包含参考列）
     *
     * @param tableName    表名
     * @param columnFamily 列族
     * @param qualifier    列限定符
     * @param columnValue  列值
     * @param count        数量
     */
    public void SingleColumnValueExcludeFilter(String tableName, String columnFamily, String qualifier, String columnValue, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();
        SingleColumnValueExcludeFilter singleColumnValueFilter = new SingleColumnValueExcludeFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(columnValue));
        //singleColumnValueFilter.setFilterIfMissing(true);//当不存在这列的行 默认不过滤
        singleColumnValueFilter.setLatestVersionOnly(true);//获取最新版本
        scan.setFilter(singleColumnValueFilter);
        scan.setCaching(10);
        //scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 行健前缀过滤器
     *
     * @param tableName 表名
     * @param rowKey    行健
     * @param count     数量
     */
    public void prefixFilter(String tableName, String rowKey, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();
        PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes(rowKey));
        scan.setFilter(prefixFilter);
        scan.setCaching(10);
        scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 分页过滤器
     *
     * @param tableName 表名
     * @param count     数量
     */
    public void pageFilter(String tableName, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();
        PageFilter pageFilter = new PageFilter(count);
        scan.setFilter(pageFilter);
        scan.setCaching(10);
        //scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 行健过滤器（只返回行健 而不返回数据）
     *
     * @param tableName 表名
     * @param count     数量
     */
    public void keyOnlyFilter(String tableName, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();
        //KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter(true);
        FirstKeyOnlyFilter keyOnlyFilter = new FirstKeyOnlyFilter();//首次行健过滤器：每行最先创建的列
        scan.setFilter(keyOnlyFilter);
        scan.setCaching(10);
        //scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 终止行过滤器 包含该结束行
     *
     * @param tableName 表名
     * @param stopRow   终止行
     * @param count     数量
     */
    public void inclusiveStopFilter(String tableName, String stopRow, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();
        InclusiveStopFilter inclusiveStopFilter = new InclusiveStopFilter(Bytes.toBytes(stopRow));
        scan.setFilter(inclusiveStopFilter);
        scan.setCaching(10);
        //scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 时间戳过滤器
     *
     * @param tableName 表名
     * @param list      时间戳集合
     * @param count     数量
     */
    public void timestampsFilter(String tableName, List<Long> list, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();

        TimestampsFilter timestampsFilter = new TimestampsFilter(list);
        scan.setFilter(timestampsFilter);
        scan.setCaching(10);
        //scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 列计数过滤器（只获取前几列数据）
     *
     * @param tableName   表名
     * @param columnCOunt 列数量
     * @param count       数量
     */
    public void columnCountGetFilter(String tableName, int columnCOunt, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();

        ColumnCountGetFilter columnCountGetFilter = new ColumnCountGetFilter(columnCOunt);
        scan.setFilter(columnCountGetFilter);
        scan.setCaching(10);
        //scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 列分页过滤器
     *
     * @param tableName 表名
     * @param limit     列数量
     * @param offset    列偏移量
     * @param count     数量
     */
    public void columnPaginationFilter(String tableName, int limit, int offset, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();

        ColumnPaginationFilter columnPaginationFilter = new ColumnPaginationFilter(limit, offset);
        scan.setFilter(columnPaginationFilter);
        scan.setCaching(10);
        //scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 列前缀过滤器
     *
     * @param tableName  表名
     * @param columnName 列名称
     * @param count      数量
     */
    public void columnPrefixFilter(String tableName, String columnName, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();

        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes(columnName));
        scan.setFilter(columnPrefixFilter);
        scan.setCaching(10);
        scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 随机行过滤器（过滤的时候会随机获取一个随机数来比对chance）
     *
     * @param tableName 表名
     * @param chance    随机数 0-1
     * @param count     数量
     */
    public void randomRowFilter(String tableName, float chance, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();

        RandomRowFilter randomRowFilter = new RandomRowFilter(chance);
        scan.setFilter(randomRowFilter);
        scan.setCaching(10);
        //scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 跳转过滤器
     *
     * @param tableName   表名
     * @param columnValue 列值
     * @param count       数量
     */
    public void skipFilter(String tableName, String columnValue, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();

        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(columnValue)));
        SkipFilter skipFilter = new SkipFilter(valueFilter);
        scan.setFilter(skipFilter);
        scan.setCaching(10);
        //scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 蜷匹配过滤器
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @param count     数量
     */
    public void whileMatchFilter(String tableName, String rowKey, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(rowKey)));
        WhileMatchFilter whileMatchFilter = new WhileMatchFilter(rowFilter);
        scan.setFilter(whileMatchFilter);
        scan.setCaching(10);
        //scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 蜷匹配过滤器
     *
     * @param tableName    表名
     * @param rowKey       行键
     * @param columnFamily 列族
     * @param columnName   列限定符
     * @param count        数量
     */
    public void filterList(String tableName, String rowKey, String columnFamily, String columnName, int count) {
        HBaseConfiguration hBaseConfiguration = new HBaseConfiguration();
        Table table = hBaseConfiguration.table(tableName);
        Scan scan = new Scan();

        List<Filter> rowFilters = new ArrayList<Filter>();
        rowFilters.add(new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes(rowKey))));//选择行健
        rowFilters.add(new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes(columnFamily))));//选择列族
        rowFilters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(columnName)));//选择列限定符
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, rowFilters);
        scan.setFilter(filterList);
        scan.setCaching(10);
        //scan.setBatch(10);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result[] results = scanner.next(count);
            HBaseResultUtil.print(results);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
