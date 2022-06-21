package org.darkphoenixs.hbase.repair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.darkphoenixs.hbase.repair.domain.MateRow;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MetaScanRunable implements Runnable {

    public static ConcurrentHashMap<String,Connection> poolMap = new ConcurrentHashMap();

    MateRow mate = null;
    AtomicInteger requesrNum = null;
    Set<String> tableScaneFail = null;
    List<String> regionNameFailArr = null;
    List<String> encodeNameFailArr = null;
    Set<String> tableScaned = null;
    Connection connFromOut = null;
    BigDecimal metasize;
    Configuration configuration;

    public MetaScanRunable(MateRow mate, AtomicInteger requesrNum, Set<String> tableScaneFail, List<String> regionNameFailArr, List<String> encodeNameFailArr, Set<String> tableScaned, Connection connFromOut, int size, Configuration configuration) {
        this.mate = mate;
        this.requesrNum = requesrNum;
        this.tableScaneFail = tableScaneFail;
        this.regionNameFailArr = regionNameFailArr;
        this.encodeNameFailArr = encodeNameFailArr;
        this.tableScaned = tableScaned;
        this.connFromOut = connFromOut;
        this.metasize = new BigDecimal(size);
        this.configuration = configuration;
    }

    @Override
    public void run(){
        String currentablename = mate.getTableName();
        MateRow currentMate = mate;
        Boolean requestOk = false;
        Result conResult = null;
        try {
            HashMap<String, Object> doscanMap = doScan();
            requestOk = (Boolean)doscanMap.get("requestOk");
            conResult = (Result)doscanMap.get("conResult");
        } catch (Exception e) {
            e.printStackTrace();
            tableScaneFail.add(currentablename);
            regionNameFailArr.add(currentMate.getRegionName());
            encodeNameFailArr.add(currentMate.getEncodedName());
            System.out.println("tableScaneFail : " + tableScaneFail);
            System.out.println("regionNameFailArr.size : " + regionNameFailArr.size());
            System.out.println("encodeNameFailArr.size : " + encodeNameFailArr.size());
            System.out.println("tableScaned : " + tableScaned);
            System.out.println("currentablename : " + currentablename);
            System.out.println("currentMateRow : " + currentMate);
            System.out.println("tableScaned.size : " + tableScaned.size());
        }
    }





    public HashMap<String, Object>  doScan( ) throws IOException {
        //零散的变量声明
        long begin = System.currentTimeMillis();
        HashMap<String, Object> map = new HashMap<>();
        map.put("requestOk",false);
        map.put("conResult",null);
        String threadname = Thread.currentThread().getName();
        //scan对象的封装
        Scan scan = new Scan();
        scan.setLimit(1);
        scan.setStartRow(mate.getStartKeyByte());
        if(mate.getEndKeyByte() != null){
            scan.setStopRow(mate.getEndKeyByte());
        }
        KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter();
        scan.setFilter(keyOnlyFilter);
        //封装table对象
        Connection connection = poolMap.get(Thread.currentThread().getName());
        if(connection == null){
            System.out.println(threadname+"新建了一个连接");
            Connection conn = ConnectionFactory.createConnection(configuration);
            poolMap.put(Thread.currentThread().getName(),conn);
            connection = conn;
        }
        Table table = connection.getTable(TableName.valueOf(mate.getTableName()));
        //开始扫描
        Iterator<Result> iterator = table.getScanner(scan).iterator();
        boolean haveResult = false;
        requesrNum.addAndGet(1);
        tableScaned.add(mate.getTableName());
        if (iterator.hasNext()) {
            Result result = iterator.next();
            if (!result.isEmpty()) {
                haveResult = true;
                String resultStr = result.toString();
                int needlength = 30;
                if (resultStr.length() > needlength) {
                    needlength = needlength;
                } else {
                    needlength = resultStr.length();
                }
                resultStr = resultStr.substring(0, needlength);
                long end = System.currentTimeMillis();
                double requesrNumint = requesrNum.get();
                BigDecimal requesrNumbig = new BigDecimal(requesrNumint*100);
                System.out.println(threadname+":"+"扫描成功: " + mate.getTableName() + " ；resultStr(前"+needlength+")：【" + resultStr+"】;进度:"+(requesrNumbig.divide(metasize,2))+"%; 耗时ms："+(end - begin));
                Boolean requestOk = true;
                Result conResult = result;
                map.put("requestOk",requestOk);
                map.put("conResult",conResult);
            }
        }
        if(!haveResult){
            long end = System.currentTimeMillis();
            double requesrNumint = requesrNum.get();
            BigDecimal requesrNumbig = new BigDecimal(requesrNumint*100);
            System.out.println(threadname+":"+"扫描成功; tableName:" + mate.getTableName() + ";进度: "+(requesrNumbig.divide(metasize,2))+"%; 耗时ms："+(end - begin));
        }
        return map;
    }





}
