// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service;

import io.micrometer.core.instrument.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.adtiming.om.adc.util.MapHelper.getInt;
import static com.adtiming.om.adc.util.MapHelper.getString;

@Service
public class InstanceChangeService {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private AppConfig cfg;

    @Scheduled(cron = "0 0 1 * * *")
    public void removeInstanceChange() {
        if (!cfg.isProd())
            return;

        LOG.info("updateInstanceChangeStatus start");
        long start = System.currentTimeMillis();
        int count = 0;
        try {
            List<Map<String, Object>> instanceList = jdbcTemplate.queryForList("SELECT a.*,b.app_id FROM om_instance_change a left join om_publisher_app b on (a.pub_app_id=b.id) WHERE a.status=1");
            List<Object[]> deleteDataParams = new ArrayList<>();
            String deleteSql = "delete from om_instance_change where id=? and placement_key=?";
            for (Map<String, Object> instance : instanceList) {
                boolean hasData = hasDelayData(instance);
                int insId = getInt(instance, "id");
                String placementKey = getString(instance, "placement_key");
                if (!hasData) {
                    deleteDataParams.add(new Object[] {insId, placementKey});
                    count++;
                }
                if (deleteDataParams.size() >= 1000) {
                    jdbcTemplate.batchUpdate(deleteSql, deleteDataParams);
                    deleteDataParams.clear();
                }
            }
            if (!deleteDataParams.isEmpty()) {
                jdbcTemplate.batchUpdate(deleteSql, deleteDataParams);
            }
        } catch (Exception e) {
            LOG.error("updateInstanceChangeStatus error", e);
        }
        LOG.info("updateInstanceChangeStatus end, removeCount:{}, cost:{}", count, System.currentTimeMillis() - start);
    }

    //7天没数删除
    private boolean hasDelayData(Map<String, Object> instance) {
        int adnId = getInt(instance, "adnId");
        String placementKey = getString(instance, "placement_key");
        String appKey = getString(instance, "app_key");
        //String appId = MapHelper.getString(instance, "app_id");
        String tableName = "";
        String whereSql = "";
        if (StringUtils.isBlank(placementKey)) {//placement_key为空删除
            return false;
        }
        switch (adnId) {
            case 2://Admob
                placementKey = placementKey.replace("/", ":");
                tableName = "report_admob";
                whereSql = String.format("ad_unit_id='%s'", placementKey);
                break;
            case 3://Facebook
                tableName = "report_facebook";
                whereSql = String.format("concat(app,'_',placement)='%s'", placementKey);
                break;
            case 4://Unity
                tableName = "report_unity";
                whereSql = String.format("concat(source_game_id,'_',source_zone)='%s'",
                        appKey + "_" + placementKey);
                break;
            case 5://Vungle
                tableName = "report_vungle";
                whereSql = String.format("placement_reference_id='%s'", placementKey);
                break;
            case 7://Adcolony
                tableName = "report_adcolony";
                whereSql = String.format("zone_id='%s'", placementKey);
                break;
            case 8://AppLovin
                tableName = "report_applovein";
                whereSql = String.format("zone_id='%s'", placementKey);
                break;
            case 9://Mopub
                tableName = "report_mopub";
                whereSql = String.format("adunit_id='%s'", placementKey);
                break;
            case 11://Tapjoy
                tableName = "report_tapjoy";
                whereSql = String.format("placement_name='%s'", placementKey);
                break;
            case 12://Chartboost
                tableName = "report_chartboost";
                whereSql = String.format("campaign_name='%s'", placementKey);
                break;
            case 13://TikTok
                tableName = "report_tiktok";
                whereSql = String.format("ad_slot_id='%s'", placementKey);
                break;
            case 14://Minitegral
                tableName = "report_mintegral";
                whereSql = String.format("unit_id='%s'", placementKey);
                break;
            default:
                break;
        }
        if (StringUtils.isNotBlank(whereSql)) {
            try {
                String sql = String.format("select count(1) from %s where %s", tableName, whereSql);
                int count = jdbcTemplate.queryForObject(sql, Integer.class);
                return count > 0;
            } catch (Exception e) {
                LOG.error("hasDelayData error", e);
            }
        }
        return true;
    }

    @Scheduled(cron = "0 0 1 * * *")
    public void deleteOldInstanceChange() {
        int count = 0;
        LOG.info("deleteOldInstanceChange start");
        int updateCount = 0;
        long start = System.currentTimeMillis();
        while (count < 5) {
            try {
                //删除4天前的变更配置
                String updateSql = "UPDATE om_instance_change SET status=3 WHERE create_time<DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 4 DAY) and status=1";
                updateCount = jdbcTemplate.update(updateSql);
                break;
            } catch (Exception e) {
                count++;
                if (count == 5) {
                    LOG.error("deleteOldInstanceChange failed", e);
                }
                try {
                    Thread.sleep(1000 * 60);
                } catch (InterruptedException ignored) {
                }
            }
        }
        LOG.info("deleteOldInstanceChange end, count:{}, cost:{}", updateCount, System.currentTimeMillis() - start);
    }

}
