// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service;

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

public class BaseTask {

    private static final Logger LOG = LogManager.getLogger();

    public void buildTask(JdbcTemplate jdbcTemplate, String[] days, int hour, int adnId, String adnName) {
        LOG.info("[{}] build task start, days:{}", adnName, days);
        long start = System.currentTimeMillis();
        try {
            String sql = "select * from report_adnetwork_account where adn_id=? and status=1" + getWhereSql(adnId);
            jdbcTemplate.query(sql, rs -> {
                Map<String, String> map = buildApiInfo(adnId, rs);
                for (String day : days) {
                    insertTask(jdbcTemplate, rs.getInt("id"), adnId, adnName, day, hour, map);
                }
            }, adnId);
        } catch (Exception e) {
            LOG.error("[{}] build task error", adnName, e);
        }
        LOG.info("[{}] build task end, days:{}, cost:{}", adnName, days, System.currentTimeMillis() - start);
    }

    public void buildTaskById(JdbcTemplate jdbcTemplate, int accountId, String[] days, int hour, int adnId, String adnName) {
        LOG.info("[{}] build task start, days:{}, accoutnId:{}", adnName, days, accountId);
        long start = System.currentTimeMillis();
        try {
            String sql = "select * from report_adnetwork_account where adn_id=? and id=? and status=1" + getWhereSql(adnId);

            jdbcTemplate.query(sql, rs -> {
                Map<String, String> map = buildApiInfo(adnId, rs);
                for (String day : days) {
                    insertTask(jdbcTemplate, accountId, adnId, adnName, day, hour, map);
                }
            }, adnId, accountId);
        } catch (Exception e) {
            LOG.error("[{}] build task error", adnName, e);
        }
        LOG.info("[{}] build task end, days:{}, cost:{}", adnName, days, System.currentTimeMillis() - start);
    }

    private String getWhereSql(int adnId) {
        String[] fileds = new String[0];
        switch (adnId) {
            case 1://adtiming
            case 7://AdColony
                fileds = new String[]{"adn_app_token"};
                break;
            case 2://admob
                fileds = new String[]{"user_id", "adn_api_key", "adn_app_token", "credential_path"};
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
                fileds = new String[]{"adn_app_id", "adn_api_key"};
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
        }
        StringBuilder whereSql = new StringBuilder();
        for (String filed : fileds) {
            whereSql.append(String.format(" and %s is not null and %s != ''", filed, filed));
        }
        return whereSql.toString();
    }

    private Map<String, String> buildApiInfo(int adnId, ResultSet rs) throws SQLException {
        Map<String, String> map = new HashMap<>();
        switch (adnId) {
            case 1://adtiming
            case 7://AdColony
                map.put("adn_app_token", rs.getString("adn_app_token"));
                break;
            case 2://admob
                map.put("user_id", rs.getString("user_id"));
                map.put("adn_api_key", rs.getString("adn_api_key"));
                map.put("adn_app_token", rs.getString("adn_app_token"));
                map.put("credential_path", rs.getString("credential_path"));
                break;
            case 3://facebook
                map.put("adn_app_id", rs.getString("adn_app_id"));
                map.put("adn_app_token", rs.getString("adn_app_token"));
                break;
            case 4://Unity
            case 5://Vungle
                map.put("adn_api_key", rs.getString("adn_api_key"));
                break;
            case 8://AppLovin
            case 9://Mopub
                map.put("adn_app_id", rs.getString("adn_app_id"));
                map.put("adn_api_key", rs.getString("adn_api_key"));
                break;
            case 11://Tapjoy
                map.put("adn_api_key", rs.getString("adn_api_key"));
                map.put("adn_app_token", rs.getString("adn_app_token"));
                break;
            case 12://Chartboost
            case 13://TikTok
                map.put("user_id", rs.getString("user_id"));
                map.put("user_signature", rs.getString("user_signature"));
                break;
            case 14://Mintegral
                map.put("adn_api_key", rs.getString("adn_api_key"));
                map.put("user_signature", rs.getString("user_signature"));
                break;
            case 15://IronSource
                map.put("user_id", rs.getString("user_id"));
                map.put("user_signature", rs.getString("user_signature"));
                break;
        }
        return map;
    }

    private void insertTask(JdbcTemplate jdbcTemplate, int accountId, int adnId, String adnName, String day, int hour, Map<String, String> apiMap) {
        try {
            List<String> symbols = new ArrayList<>();
            Object[] obj = new Object[5 + apiMap.size()];
            obj[0] = day;
            obj[1] = hour;
            obj[2] = accountId;
            obj[3] = adnId;
            obj[4] = 0;
            String[] vals = apiMap.values().toArray(new String[0]);
            for (int i = 0; i < apiMap.size(); i++) {
                symbols.add("?");
                obj[4 + i + 1] = vals[i];
            }
            String insertSql = String.format("insert into report_adnetwork_task(day,hour,report_account_id,adn_id,status,%s)values(?,?,?,?,?,%s)", StringUtils.join(apiMap.keySet(), ","), StringUtils.join(symbols, ","));
            jdbcTemplate.update(insertSql, obj);
        } catch (Exception e) {
            LOG.error("[{}] insertTask error", adnName, e);
        }
    }
}
