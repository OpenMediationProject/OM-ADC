// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.facebook;

import com.adtiming.om.adc.dto.ReportAccount;
import com.adtiming.om.adc.dto.ReportApiError;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.service.BaseTask;
import com.adtiming.om.adc.util.DateTimeFormat;
import com.adtiming.om.adc.util.Http;
import com.adtiming.om.adc.util.JdbcHelper;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Service
public class RequestFacebook extends BaseTask {

    private static final Logger LOG = LogManager.getLogger();
    private static final String columns = "['fb_ad_network_revenue','fb_ad_network_request','fb_ad_network_cpm','fb_ad_network_click','fb_ad_network_imp','fb_ad_network_filled_request','fb_ad_network_fill_rate','fb_ad_network_ctr','fb_ad_network_show_rate','fb_ad_network_video_guarantee_revenue','fb_ad_network_video_view','fb_ad_network_video_view_rate','fb_ad_network_video_mrc','fb_ad_network_video_mrc_rate','fb_ad_network_bidding_request','fb_ad_network_bidding_response']";
    private static final String breakdowns = "['country','app','placement','platform']";
    private static final String url = "https://graph.facebook.com/v2.11/%s/adnetworkanalytics/?since=%s&until=%s&aggregation_period=day&metrics=%s&breakdowns=%s&access_token=%s";

    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private DownloadFacebook downloadFacebook;


    private final int TIME_DELAY = -17;

    @Scheduled(cron = "0 19 * * * ?")
    public void buildTask() {
        if (!cfg.isProd())
            return;

        LOG.info("[Facebook] buildTask Run Start");
        long start = System.currentTimeMillis();
        try {
            //获取17小时前的当天数据
            String day = DateTimeFormat.DAY_FORMAT.format(LocalDateTime.now().plusHours(TIME_DELAY).toLocalDate());
            String sql = "select * from report_adnetwork_account where adn_id=3 and status=1";
            jdbcTemplate.query(sql, rs -> {
                ReportAccount conf = ReportAccount.ROWMAPPER.mapRow(rs, 1);
                buildQueryTask(conf, day);
            });
        } catch (Exception e) {
            LOG.error("[Facebook] buildTask error", e);
        }
        LOG.info("[Facebook] buildTask Run End, cost;{}", System.currentTimeMillis() - start);
    }

    //重跑昨天和前天数据
    @Scheduled(cron = "0 39 0 * * ?")
    public void buildTesterdayTask() {
        if (!cfg.isProd())
            return;
        LOG.info("[Facebook] Rerun data from 2 days before, Start");
        long start = System.currentTimeMillis();
        try {
            String yesterday = DateTimeFormat.DAY_FORMAT.format(LocalDateTime.now().plusHours(TIME_DELAY).plusDays(-1).toLocalDate());
            String beforeYesterday = DateTimeFormat.DAY_FORMAT.format(LocalDateTime.now().plusHours(TIME_DELAY).plusDays(-2).toLocalDate());
            String sql = "select * from report_adnetwork_account where adn_id=3 and status=1";
            jdbcTemplate.query(sql, rs -> {
                ReportAccount conf = ReportAccount.ROWMAPPER.mapRow(rs, 1);
                buildQueryTask(conf, yesterday);
                buildQueryTask(conf, beforeYesterday);
            });
        } catch (Exception e) {
            LOG.error("[Facebook] buildTask error", e);
        }
        LOG.info("[Facebook] Rerun data from 2 days before, End, cost;{}", System.currentTimeMillis() - start);
    }

    private void buildQueryTask(ReportAccount conf, String day) {
        String appId = conf.adnAppId;
        String reqUrl = String.format(url, appId, day, day, columns, breakdowns, conf.adnAppToken);
        String error = "";
        try {
            String resp = Http.post(reqUrl, 60000, cfg.httpProxy);
            if (StringUtils.isNoneBlank(resp)) {
                JSONObject obj = JSON.parseObject(resp);
                String queryId = obj.getString("query_id");
                String reqDataUrl = obj.getString("async_result_link");
                if (StringUtils.isBlank(queryId)) {
                    error = obj.getString("error");
                    LOG.error("facebook get queryId error, app_id:{},day:{},reqUrl:{},error:{}", appId, day, reqUrl, obj.getString("error"));
                } else {
                    try {
                        String sql = "INSERT INTO report_adnetwork_task (day,hour,report_account_id,query_id,adn_app_id,adn_app_token,req_url,adn_id,status)  values(?,0,?,?,?,?,?,3,0)";
                        jdbcTemplate.update(sql, day, conf.id, queryId, appId, conf.adnAppToken, reqDataUrl);
                    } catch (Exception e) {
                        LOG.error("insert report_task error,app_id:{},day:{},req_url:{},error:", appId, day, reqUrl, e);
                        error = "Failed to create pull task. Reason is " + e.getMessage();
                    }
                }
            } else {
                error = "The API request returns an empty string.";
                LOG.error("[facebook] get query_id resp is null, report_account_id:{},day:{},req_url:{}", conf.id, day, reqUrl);
            }
        } catch (Exception e) {
            error = e.getMessage();
            LOG.error("[facebook] buildQueryTask error,app_id:{},day:{},req_url:{},error:", appId, day, reqUrl, e);
        }
        if (StringUtils.isNoneBlank(error)) {
            ReportApiError apiError = downloadFacebook.matchApiError(error);
            if (apiError != null) {
                if (!apiError.isIgnore()) {//非忽略错误,更新账号状态为Exception
                    jdbcTemplate.update("UPDATE report_adnetwork_account SET status=2,reason=?,error_id=? WHERE id=?",
                            error, apiError.id, conf.id);
                } else {
                    //发生忽略错误告警
                    LOG.error("[Facebook] ignore error has occurred,report_account_id:{},error:{}", conf.id, error);
                    //出现新错误告警
                    jdbcTemplate.update("UPDATE report_adnetwork_account SET reason=?,error_id=? WHERE id=?",
                            error, apiError.id, conf.id);
                }
            } else {
                //出现新错误告警
                long errorId = JdbcHelper.insertReturnId(jdbcTemplate,"insert into report_adnetwork_error(adn_id,content)values(?,?)", 2, error);
                jdbcTemplate.update("UPDATE report_adnetwork_account SET reason=?,error_id=? WHERE id=?",
                        error, errorId, conf.id);
                LOG.error("[Facebook] add new error info,report_account_id:{},error:{}", conf.id, error);
            }
        }
    }

    public void rebuildTask(String[] days, int reportAdnAccountId) {
        ReportAccount conf = jdbcTemplate.queryForObject("select * from report_adnetwork_account where adn_id=3 and status=1 and id=?", ReportAccount.ROWMAPPER, reportAdnAccountId);
        Map<String, Map<String, Object>> taskMap = getFailedTask(jdbcTemplate, days, 0, 3);
        for (String day : days) {
            String key = conf.adnAppId + "_" + day + "_0";
            if (!taskMap.containsKey(key)) {
                buildQueryTask(conf, day);
            }
        }
    }

    public void rebuildTask(String[] days) {
        List<ReportAccount> list = jdbcTemplate.query("select * from report_adnetwork_account where adn_id=3 and status=1", ReportAccount.ROWMAPPER);
        Map<String, Map<String, Object>> taskMap = getFailedTask(jdbcTemplate, days, 0, 3);
        for (String day : days) {
            for (ReportAccount conf : list) {
                String key = conf.adnAppId + "_" + day + "_0";
                if (!taskMap.containsKey(key)) {
                    buildQueryTask(conf, day);
                }
            }
        }
    }
}
