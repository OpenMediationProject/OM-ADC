// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service;

import com.adtiming.om.adc.util.MapHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BaseTask {

    private static final Logger LOG = LogManager.getLogger();

    public void buildTask(JdbcTemplate jdbcTemplateW, String[] days, int hour, int adnId, String adnName, int timeDimension) {
        LOG.info("[{}] build task start, days:{}", adnName, days);
        long start = System.currentTimeMillis();
        try {
            String keyField = getKeyField(adnId);
            Map<String, Map<String, Object>> taskMap = getFailedTask(jdbcTemplateW, days, hour, adnId);
            String sql = "select * from report_adnetwork_account where adn_id=? and status in (-1,1)" + getWhereSql(adnId);
            jdbcTemplateW.query(sql, rs -> {
                Map<String, Object> map = buildApiInfo(adnId, rs);
                int accountId = rs.getInt("id");
                for (String day : days) {
                    String key = rs.getString(keyField) + "_" + day + "_" + hour;
                    if (!taskMap.containsKey(key)) {
                        insertTask(jdbcTemplateW, accountId, adnId, adnName, day, hour, map, timeDimension);
                    }
                }
            }, adnId);
        } catch (Exception e) {
            LOG.error("[{}] build task error", adnName, e);
        }
        LOG.info("[{}] build task end, days:{}, cost:{}", adnName, days, System.currentTimeMillis() - start);
    }

    public void buildTask(JdbcTemplate jdbcTemplateW, String[] days, int hour, int adnId, String adnName) {
        LOG.info("[{}] build task start, days:{}", adnName, days);
        long start = System.currentTimeMillis();
        try {
            String keyField = getKeyField(adnId);
            Map<String, Map<String, Object>> taskMap = getFailedTask(jdbcTemplateW, days, hour, adnId);
            String sql = "select * from report_adnetwork_account where adn_id=? and status in (-1,1)" + getWhereSql(adnId);
            jdbcTemplateW.query(sql, rs -> {
                Map<String, Object> map = buildApiInfo(adnId, rs);
                int accountId = rs.getInt("id");
                for (String day : days) {
                    String key = rs.getString(keyField) + "_" + day + "_" + hour;
                    if (!taskMap.containsKey(key)) {
                        insertTask(jdbcTemplateW, accountId, adnId, adnName, day, hour, map, 1);
                    }
                }
            }, adnId);
        } catch (Exception e) {
            LOG.error("[{}] build task error", adnName, e);
        }
        LOG.info("[{}] build task end, days:{}, cost:{}", adnName, days, System.currentTimeMillis() - start);
    }

    protected void buildTaskById(JdbcTemplate jdbcTemplateW, int id, String[] days, int hour, int adnId, String adnName, int timeDimension) {
        LOG.info("[{}] build task start, days:{}", adnName, days);
        long start = System.currentTimeMillis();
        try {
            String keyField = getKeyField(adnId);
            Map<String, Map<String, Object>> taskMap = getFailedTask(jdbcTemplateW, days, hour, adnId);

            String sql = "select * from report_adnetwork_account where adn_id=? and id=? and status in (-1,1)" + getWhereSql(adnId);
            jdbcTemplateW.query(sql, rs -> {
                Map<String, Object> map = buildApiInfo(adnId, rs);
                int accountId = 0;
                if (adnId != 6) {
                    accountId = rs.getInt("id");
                }
                for (String day : days) {
                    String key = rs.getString(keyField) + "_" + day + "_" + hour;
                    if (!taskMap.containsKey(key)) {
                        insertTask(jdbcTemplateW, accountId, adnId, adnName, day, hour, map, timeDimension);
                    }
                }
            }, adnId, id);
        } catch (Exception e) {
            LOG.error("[{}] build task error", adnName, e);
        }
        LOG.info("[{}] build task end, days:{}, cost:{}", adnName, days, System.currentTimeMillis() - start);
    }

    protected void buildTaskById(JdbcTemplate jdbcTemplateW, int id, String[] days, int hour, int adnId, String adnName) {
        LOG.info("[{}] build task start, days:{}", adnName, days);
        long start = System.currentTimeMillis();
        try {
            String keyField = getKeyField(adnId);
            Map<String, Map<String, Object>> taskMap = getFailedTask(jdbcTemplateW, days, hour, adnId);

            String sql = "select * from report_adnetwork_account where adn_id=? and id=? and status in (-1,1)" + getWhereSql(adnId);
            jdbcTemplateW.query(sql, rs -> {
                Map<String, Object> map = buildApiInfo(adnId, rs);
                for (String day : days) {
                    String key = rs.getString(keyField) + "_" + day + "_" + hour;
                    if (!taskMap.containsKey(key)) {
                        insertTask(jdbcTemplateW, rs.getInt("id"), adnId, adnName, day, hour, map, 1);
                    }
                }
            }, adnId, id);
        } catch (Exception e) {
            LOG.error("[{}] build task error", adnName, e);
        }
        LOG.info("[{}] build task end, days:{}, cost:{}", adnName, days, System.currentTimeMillis() - start);
    }

    protected Map<String, Map<String, Object>> getFailedTask(JdbcTemplate jdbcTemplateW, String[] days, int hour, int adnId) {
        String failedTaskSql = String.format("select distinct day,hour,adn_id,adn_app_id,adn_api_key,adn_app_token,user_id,user_signature from report_adnetwork_task where adn_id=? and status!=2 and day in ('%s') and hour=?", StringUtils.join(days, "','"));
        String keyField = getKeyField(adnId);
        Map<String, Map<String, Object>> taskMap = jdbcTemplateW.queryForList(failedTaskSql, adnId, hour).stream()
                .collect(Collectors.toMap(o -> MapHelper.getString(o, keyField)
                                + "_" + MapHelper.getString(o, "day")
                                + "_" + MapHelper.getInt(o, "hour"),
                        o -> o));
        return taskMap;
    }

    private String getKeyField(int adnId) {
        String keyField = "";
        switch (adnId) {
            case 2://admob
            case 12://Chartboost
            case 13://TikTok
            case 15://IronSource
            case 17://Helium
                keyField = "user_id";
                break;
            case 3://facebook
                keyField = "adn_app_id";
                break;
            case 7://AdColony
            case 18:
                keyField = "adn_app_token";
                break;
            case 4://Unity
            case 5://Vungle
            case 8://AppLovin
            case 9://Mopub
            case 11://Tapjoy
            case 14://Mintegral
                keyField = "adn_api_key";
                break;
            default:
                break;
        }
        return keyField;
    }

    private String getWhereSql(int adnId) {
        String[] fileds = new String[0];
        switch (adnId) {
            case 1://adtiming
            case 7://AdColony
            case 18: //Mint
                fileds = new String[]{"adn_app_token"};
                break;
            case 2://admob
                fileds = new String[]{"user_id", "adn_app_token"};
                break;
            case 3://facebook
                fileds = new String[]{"adn_app_id", "adn_app_token"};
                break;
            case 4://Unity
            case 5://Vungle
                fileds = new String[]{"adn_api_key"};
                break;
            case 8://AppLovin
            case 9://Mopub
                fileds = new String[]{"adn_api_key"};
                break;
            case 11://Tapjoy
                fileds = new String[]{"adn_api_key", "adn_app_token"};
                break;
            case 12://Chartboost
            case 13://TikTok
                fileds = new String[]{"user_id", "user_signature"};
                break;
            case 14://Mintegral
                fileds = new String[]{"adn_api_key", "user_signature"};
                break;
            case 15://IronSource
                fileds = new String[]{"user_id", "user_signature"};
                break;
        }
        StringBuilder whereSql = new StringBuilder();
        for (String filed : fileds) {
            whereSql.append(String.format(" and %s is not null and %s != ''", filed, filed));
        }
        return whereSql.toString();
    }

    private Map<String, Object> buildApiInfo(int adnId, ResultSet rs) throws SQLException {
        Map<String, Object> map = new HashMap<>();
        map.put("adn_app_id", rs.getString("adn_app_id"));
        map.put("adn_api_key", rs.getString("adn_api_key"));
        map.put("adn_app_token", rs.getString("adn_app_token"));
        map.put("user_id", rs.getString("user_id"));
        map.put("user_signature", rs.getString("user_signature"));
        map.put("credential_path", rs.getString("credential_path"));
        map.put("auth_type", rs.getInt("auth_type"));
        map.put("currency", rs.getString("currency"));
        return map;
    }

    private void insertTask(JdbcTemplate jdbcTemplateW, int accountId, int adnId, String adnName, String day, int hour, Map<String, Object> apiMap, int timeDimension) {
        try {
            List<String> symbols = new ArrayList<>();
            Object[] obj = new Object[6 + apiMap.size()];
            obj[0] = day;
            obj[1] = hour;
            obj[2] = accountId;
            obj[3] = adnId;
            obj[4] = 0;
            obj[5] = timeDimension;
            Object[] vals = apiMap.values().toArray(new Object[0]);
            for (int i = 0; i < apiMap.size(); i++) {
                symbols.add("?");
                obj[5 + i + 1] = vals[i];
            }
            String insertSql = String.format("insert into report_adnetwork_task(day,hour,report_account_id,adn_id,status,time_dimension,%s)values(?,?,?,?,?,?,%s)", StringUtils.join(apiMap.keySet(), ","), StringUtils.join(symbols, ","));
            jdbcTemplateW.update(insertSql, obj);
        } catch (Exception e) {
            LOG.error("[{}] insertTask error", adnName, e);
        }
    }
}
