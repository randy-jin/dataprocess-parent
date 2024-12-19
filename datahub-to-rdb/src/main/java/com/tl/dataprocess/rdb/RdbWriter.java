package com.tl.dataprocess.rdb;

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.FieldType;
import com.aliyun.datahub.exception.OffsetResetedException;
import com.aliyun.datahub.model.OffsetContext;
import com.aliyun.datahub.model.RecordEntry;
import com.tl.dataprocess.datahub.DataHubShardCache;
import com.tl.dataprocess.datahub.SingleSubscriptionAsyncExecutor;
import com.tl.util.TaskThreadPool;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.Executors;

/**
 * Created by jinzhiqiang on 2021/9/13.
 * 从DataHub订阅数据,并写入关系数据库
 */
public class RdbWriter extends SingleSubscriptionAsyncExecutor {
    Logger logger = LoggerFactory.getLogger(RdbWriter.class);

    private JdbcTemplate jdbcTemplate;

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private Map<String, String[]> colNameTypeMap;
    private Map<String, Boolean> notNullFieldMap;

    private String sqlPrefix;
    private String tableName;
    private String dbName;

    public String getSqlPrefix() {
        return sqlPrefix;
    }

    public void setSqlPrefix(String sqlPrefix) {
        this.sqlPrefix = sqlPrefix;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    @Override
    protected void init() {
        if (isRun == 0) {
            return;
        }
        final String fieldSQL = "select UPPER(COLUMN_NAME) as COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,EXTRA from INFORMATION_SCHEMA.Columns where table_name='" + tableName + "' and table_schema='" + dbName + "'";
        List<Map<String, Object>> fieldList = jdbcTemplate.queryForList(fieldSQL);
        colNameTypeMap = new HashMap<String, String[]>(fieldList.size());
        notNullFieldMap = new HashMap<String, Boolean>(fieldList.size());
        for (Map<String, Object> map : fieldList) {
            String columnName = String.valueOf(map.get("COLUMN_NAME")).toUpperCase();
            String columnType = StringUtils.substringBefore(String.valueOf(map.get("COLUMN_TYPE")), "(");
            String isNullable = String.valueOf(map.get("IS_NULLABLE"));
            String extra = String.valueOf(map.get("EXTRA"));
            boolean isNullAble = isNullable.equals("NO") ? false : true;
            if (!isNullAble) {//字段不允许为空，加入非空集合
                notNullFieldMap.put(columnName, isNullAble);
            }
            if (extra.equals("auto_increment")) {
                continue;
            }
            colNameTypeMap.put(columnName, new String[]{columnType, isNullable, extra});
        }
        List<String> activeShardList = DataHubShardCache.getActiveShard(datahubClient, datahubPojectName, datahubTopicName);
        executor = Executors.newFixedThreadPool(threadPoolSize == 0 ? activeShardList.size() : threadPoolSize, new TaskThreadPool(datahubTopicName));
        for (final String activeShard : activeShardList) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    start(activeShard);
                }
            });
        }
    }

    @Override
    protected long dataProcess(OffsetContext offsetCtx, long recordNum, List<RecordEntry> records) {
        try {
            Random random = new Random();
            final int id = random.nextInt();
            logger.info(id + " records size " + records.size());
            final List<Map<String, Object>> fieldValueList = new ArrayList<>(records.size());
            for (RecordEntry record : records) {
                offsetCtx.setOffset(record.getOffset());
                Field[] fields = record.getFields();
                Map<String, Object> fieldValue = new HashMap<String, Object>();
                for (Field field : fields) {
                    String fieldName = field.getName();
                    String fieldNameUpper = fieldName.toUpperCase();
                    if (field.getType() == FieldType.BIGINT) {
                        fieldValue.put(fieldNameUpper, record.getBigint(fieldName));
                    } else if (field.getType() == FieldType.BOOLEAN) {
                        fieldValue.put(fieldNameUpper, record.getBoolean(fieldName));
                    } else if (field.getType() == FieldType.DOUBLE) {
                        fieldValue.put(fieldNameUpper, record.getDouble(fieldName));
                    } else if (field.getType() == FieldType.TIMESTAMP) {
                        java.util.Date val = record.getTimeStampAsDate(fieldName);
                        if (null == val && notNullFieldMap.containsKey(fieldNameUpper)) {
                            break;
                        }
                        fieldValue.put(fieldNameUpper, val);
                    } else if (field.getType() == FieldType.STRING) {
                        String val = record.getString(fieldName);
                        if (null == val && notNullFieldMap.containsKey(fieldNameUpper)) {
                            break;
                        }
                        fieldValue.put(fieldNameUpper, val);
                    } else if (field.getType() == FieldType.DECIMAL) {
                        BigDecimal val = record.getDecimal(fieldName);
                        if (null == val && notNullFieldMap.containsKey(fieldNameUpper)) {
                            break;
                        }
                        fieldValue.put(fieldNameUpper, val);
                    } else {
                        throw new IllegalArgumentException("Unknown record type :" + field.getType().name());
                    }
                }
                fieldValueList.add(fieldValue);
            }

            // logger.info(id+" fieldValueList size:" + records.size());

            final StringBuilder colNames = new StringBuilder();
            StringBuilder params = new StringBuilder();
            Iterator<Map.Entry<String, String[]>> iter = colNameTypeMap.entrySet().iterator();
            if (fieldValueList.size() == 0) {
                return 0L;
            }
            Map<String, Object> fieldValueTemplate = fieldValueList.get(0);
            while (iter.hasNext()) {
                Map.Entry<String, String[]> entry = iter.next();
                String colName = entry.getKey();
                // 修改于2021年6月30日下午,为应对井井通订阅失效问题,该逻辑放到init()方法的109行进行处理.
//                if (colNameTypeMap.get(colName)[2].equals("auto_increment")) {
//                    // 移除自增字段
//                    colNameTypeMap.remove(colName);
//                    continue;
//                }
                if (!fieldValueTemplate.containsKey(colName)) {
                    throw new RuntimeException("Unknown column(in rdb, but not in datahub, and is not auto_increment column):" + colName);
                }
                colNames.append(colName);
                colNames.append(",");
                params.append("?");
                params.append(",");
            }
            colNames.deleteCharAt(colNames.length() - 1);
            params.deleteCharAt(params.length() - 1);

            BatchPreparedStatementSetter batchPreparedStatementSetter = new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    Map<String, Object> fieldValue = fieldValueList.get(i);
                    int index = 0;
                    Iterator<Map.Entry<String, String[]>> iter = colNameTypeMap.entrySet().iterator();
                    while (iter.hasNext()) {
                        Map.Entry<String, String[]> entry = iter.next();
                        String colName = entry.getKey();
                        String colType = entry.getValue()[0];
                        boolean isNullAble = entry.getValue()[1].equals("NO") ? false : true;
                        Object fieldVal = fieldValue.get(colName);
                        if (null == fieldVal && !isNullAble) {
                            logger.error("Column Name [" + colName + "] can not be nullable, but the value is null");
                        }
                        if (colType.equals("date")) {
                            if (null == fieldVal) {
                                ps.setNull(++index, Types.DATE);
                            } else {
                                String s = String.valueOf(fieldVal);

                                final int YEAR_LENGTH = 4;
                                final int MONTH_LENGTH = 2;
                                final int DAY_LENGTH = 2;
                                final int MAX_MONTH = 12;
                                final int MAX_DAY = 31;
                                int firstDash;
                                int secondDash;
                                java.sql.Date d = new java.sql.Date(1000 - 1900, 10 - 1, 10);
                                ;
                                firstDash = s.indexOf('-');
                                secondDash = s.indexOf('-', firstDash + 1);

                                String yyyy = null;
                                String mm = null;
                                String dd = null;

                                if (firstDash < 0) {
                                    yyyy = s.substring(0, 4);
                                    mm = s.substring(4, 6);
                                    dd = s.substring(6);
                                } else if ((firstDash > 0) && (secondDash > 0) && (secondDash < s.length() - 1)) {
                                    yyyy = s.substring(0, firstDash);
                                    mm = s.substring(firstDash + 1, secondDash);
                                    dd = s.substring(secondDash + 1);
                                } else {
                                    logger.error("The date value is [" + s + "],will be initialized to 1000-10-10");
                                    yyyy = "1000";
                                    mm = "10";
                                    dd = "10";
                                }

                                if (yyyy.length() == YEAR_LENGTH &&
                                        (mm.length() >= 1 && mm.length() <= MONTH_LENGTH) &&
                                        (dd.length() >= 1 && dd.length() <= DAY_LENGTH)) {
                                    int year = Integer.parseInt(yyyy);
                                    int month = Integer.parseInt(mm);
                                    int day = Integer.parseInt(dd);

                                    if ((month >= 1 && month <= MAX_MONTH) && (day >= 1 && day <= MAX_DAY)) {
                                        d = new java.sql.Date(year - 1900, month - 1, day);
                                    } else {
                                        logger.error("The date value is year=[" + year + "],mouth=[" + month + "],day=[" + day + "]");
                                    }
                                }

                                ps.setDate(++index, d);
                            }
                        } else if (colType.equals("datetime")) {
                            if (null == fieldVal) {
                                ps.setNull(++index, Types.DATE);
                            } else {
                                java.util.Date date = (java.util.Date) fieldVal;
                                ps.setTimestamp(++index, new Timestamp(date.getTime()));
                            }
                        } else if (colType.equals("bigint")) {
                            if (null == fieldVal) {
                                ps.setNull(++index, Types.BIGINT);
                            } else {
                                ps.setLong(++index, (Long) fieldVal);
                            }
                        } else if (colType.equals("decimal")) {
                            if (null == fieldVal) {
                                ps.setNull(++index, Types.DECIMAL);
                            } else {
                                ps.setDouble(++index, (Double) fieldVal);
                            }
                        } else if (colType.equals("int")) {
                            if (null == fieldVal) {
                                ps.setNull(++index, Types.INTEGER);
                            } else {
                                ps.setInt(++index, Integer.parseInt(String.valueOf(fieldVal)));
                            }
                        } else if (colType.equals("text")) {
                            if (null == fieldVal) {
                                ps.setNull(++index, Types.VARCHAR);
                            } else {
                                ps.setString(++index, (String) fieldVal);
                            }
                        } else if (colType.equals("varchar")) {
                            if (null == fieldVal) {
                                ps.setNull(++index, Types.VARCHAR);
                            } else {
                                ps.setString(++index, (String) fieldVal);
                            }
                        } else if (colType.equals("tinyint")) {
                            if (null == fieldVal) {
                                ps.setNull(++index, Types.TINYINT);
                            } else {
                                ps.setInt(++index, Integer.parseInt(String.valueOf(fieldVal)));
                            }
                        } else {
                            throw new RuntimeException("Unknown column type:" + colType);
                        }
                    }
                }

                @Override
                public int getBatchSize() {
                    return fieldValueList.size();
                }
            };

            String insertSQL = sqlPrefix + " " + tableName + "(" + colNames + ") values (" + params + ")";
            int batchSuccess = 0;

            int[] counts = jdbcTemplate.batchUpdate(insertSQL, batchPreparedStatementSetter);
            for (int count : counts) {
                if (count == 1) {
                    batchSuccess++;
                }
            }
            logger.info(id + " batch updated count " + batchSuccess);
            recordNum = recordNum + batchSuccess;
        } catch (Exception e) {
            logger.error("BatchUpdate rdb error:", e);
        } finally {
            try {
                datahubClient.commitOffset(offsetCtx);
            } catch (Exception e) {
                throw new OffsetResetedException("提交偏移量时发生异常！");
            }
        }

        return recordNum;
    }
}
