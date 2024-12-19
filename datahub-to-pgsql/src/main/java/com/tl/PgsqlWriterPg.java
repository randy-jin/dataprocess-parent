package com.tl;

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
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jinzhiqiang on 2021/9/13.
 * 从DataHub订阅数据,并写入关系数据库
 */
public class PgsqlWriterPg extends SingleSubscriptionAsyncExecutor {
    Logger logger = LoggerFactory.getLogger(PgsqlWriterPg.class);

    static String regex = ".*[A-Z]+.*";

    static Pattern compile = Pattern.compile(regex);

    private JdbcTemplate jdbcTemplate;
    private Map<String, String[]> colNameTypeMap;
    private Map<String, Boolean> notNullFieldMap;
    private String pkString;
    private StringBuilder stringBuilder = null;

    private String sqlPrefix;
    private String tableName;
    private String dbName;

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

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
        final String fieldSQL = "SELECT COLUMN_NAME,udt_name as COLUMN_TYPE,IS_NULLABLE,column_default as EXTRA FROM information_schema.columns WHERE table_name ='" + tableName + "' and table_catalog='" + dbName + "'";
        List<Map<String, Object>> fieldList = jdbcTemplate.queryForList(fieldSQL);
        colNameTypeMap = new HashMap<>(fieldList.size());
        notNullFieldMap = new HashMap<>(fieldList.size());
        for (Map<String, Object> map : fieldList) {
            String columnName = String.valueOf(map.get("COLUMN_NAME")).toUpperCase();
            String columnType = StringUtils.substringBefore(String.valueOf(map.get("COLUMN_TYPE")), "(");
            String isNullable = String.valueOf(map.get("IS_NULLABLE"));
            String extra = String.valueOf(map.get("EXTRA"));
            boolean isNullAble = isNullable.equals("NO") ? false : true;
            if (!isNullAble) {//字段不允许为空，加入非空集合
                notNullFieldMap.put(columnName, isNullAble);
            }
            if (extra != null && extra.contains("nextval")) {
                continue;
            }
            colNameTypeMap.put(columnName, new String[]{columnType, isNullable, extra});
        }
        String pkSql = "";
        pkString = "\"ID\",\"DATA_TYPE\",\"DATA_DATE\"";
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
                Map<String, Object> fieldValue = new HashMap<>();
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
                        Date val = record.getTimeStampAsDate(fieldName);
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
                Matcher matcher = compile.matcher(colName);
                boolean matches = matcher.matches();
                if (matches) {
                    colNames.append("\"" + colName + "\"");
                } else {
                    colNames.append(colName);
                }
                colNames.append(",");
                params.append("?");
                params.append(",");
            }
            colNames.deleteCharAt(colNames.length() - 1);
            params.deleteCharAt(params.length() - 1);

            List<Object[]> objects = new ArrayList<>();
            for (Map<String, Object> fieldValue : fieldValueList) {
                try {
                    Set<Map.Entry<String, String[]>> entries = colNameTypeMap.entrySet();
                    int size = entries.size();
                    Iterator<Map.Entry<String, String[]>> iters = entries.iterator();
                    Object[] object = new Object[size];
                    int index = -1;
                    while (iters.hasNext()) {
                        Map.Entry<String, String[]> entry = iters.next();
                        String colName = entry.getKey();
                        String colType = entry.getValue()[0];
                        boolean isNullAble = entry.getValue()[1].equals("NO") ? false : true;
                        Object fieldVal = fieldValue.get(colName);
                        if (null == fieldVal && !isNullAble) {
                            logger.error("Column Name [" + colName + "] can not be nullable, but the value is null");
                        }
                        if (colType.equals("date")) {
                            if (null == fieldVal) {
                                object[++index] = null;
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
                                firstDash = s.indexOf('-');
                                secondDash = s.indexOf('-', firstDash + 1);

                                String yyyy;
                                String mm;
                                String dd;

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
                                object[++index] = d;
                            }
                        } else if (colType.equals("timestamp")) {
                            if (null == fieldVal) {
                                object[++index] = null;
                            } else {
                                Date date = (Date) fieldVal;
                                object[++index] = new Timestamp(date.getTime());
                            }
                        } else if (colType.equals("int8")) {
                            if (null == fieldVal) {
                                object[++index] = null;
                            } else {
                                object[++index] = fieldVal;
                            }
                        } else if (colType.equals("numeric")) {
                            if (null == fieldVal) {
                                object[++index] = null;
                            } else {
                                object[++index] = fieldVal;
                            }
                        } else if (colType.equals("int2") || "int4".equals(colType)) {
                            if (null == fieldVal) {
                                object[++index] = null;
                            } else {
                                object[++index] = Integer.parseInt(String.valueOf(fieldVal));
                            }
                        } else if (colType.equals("text")) {
                            if (null == fieldVal) {
                                object[++index] = null;
                            } else {
                                object[++index] = fieldVal;
                            }
                        } else if (colType.equals("varchar")) {
                            if (null == fieldVal) {
                                object[++index] = null;
                            } else {
                                object[++index] = fieldVal;
                            }
                        } else if (colType.equals("tinyint")) {
                            if (null == fieldVal) {
                                object[++index] = null;
                            } else {
                                object[++index] = Integer.parseInt(String.valueOf(fieldVal));
                            }
                        } else {
                            throw new RuntimeException("Unknown column type:" + colType);
                        }

                    }
                    objects.add(object);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            String insertSQL;
            StringBuilder replaceSQL = new StringBuilder();
            replaceSQL.append("insert into ").append(tableName).append("(").append(colNames).append(") values (")
                    .append(params).append(")");
            if (!"insert into".equals(sqlPrefix)) {
                if (stringBuilder == null) {
                    stringBuilder = new StringBuilder();
                    stringBuilder.append(" ON conflict(").append(pkString).append(
                            ")");
                    if ("replace into".equals(sqlPrefix)) {
                        stringBuilder.append(" DO UPDATE SET ");
                        String[] split = colNames.toString().split(",");
                        for (String s : split) {
                            stringBuilder.append(s).append("=EXCLUDED.").append(s).append(",");
                        }
                        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
                    } else {
                        stringBuilder.append(" DO nothing");
                    }
                }
                insertSQL = replaceSQL.append(stringBuilder).toString();
            } else {
                insertSQL = replaceSQL.toString();
            }

            int batchSuccess = 0;

            int[] counts = jdbcTemplate.batchUpdate(insertSQL, objects);
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