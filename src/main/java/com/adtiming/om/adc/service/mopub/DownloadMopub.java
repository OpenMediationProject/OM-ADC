// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.mopub;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.util.MapHelper;
import com.adtiming.om.adc.util.MyHttpClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tomcat.util.http.fileupload.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class DownloadMopub extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Value("${download.dir}")
    private String downloadDir;

    @Resource
    private AppConfig cfg;

    @Override
    public void setAdnInfo() {
        this.adnId = 9;
        this.adnName = "Mopub";
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            executeMopubTask(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id, e);
        }
    }

    private void executeMopubTask(ReportTask task) {
        String appKey = task.adnAppId;
        String apiKey = task.adnApiKey;
        String day = task.day;
        // Set up AdSense Management API client.
        if (StringUtils.isBlank(appKey) || StringUtils.isBlank(apiKey)) {
            LOG.error("Mopub，appKey or reportKey is null");
            return;
        }
        LOG.info("[Mopub] executeTaskImpl start, apiKey:{}, day:{}", appKey, day);
        long start = System.currentTimeMillis();
        updateTaskStatus(jdbcTemplate, task.id, 1, "");
        StringBuilder err = new StringBuilder();
        String error;
        String file_path = downCsvFile(task.id, appKey, apiKey, day, err);
        if (StringUtils.isNotBlank(file_path) && err.length() == 0) {
            error = readCsvFile(file_path, day, appKey, apiKey);
            if (StringUtils.isBlank(error)) {
                error = savePrepareReportData(task, day, appKey);
                if (StringUtils.isBlank(error)) {
                    error = reportLinkedToStat(task, appKey);
                }
            }
        } else {
            error = err.toString();
        }
        int status = StringUtils.isBlank(error) || "data is null".equals(error) ? 2 : 3;
        if (task.runCount > 5 && status != 2) {
            updateAccountException(jdbcTemplate, task.reportAccountId, error);
            LOG.error("[Mopub] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
        }
        updateTaskStatus(jdbcTemplate, task.id, status, error);
        LOG.info("[Mopub] executeTaskImpl end, appKey:{}, apiKey:{}, day:{}, cost:{}", appKey, apiKey, day, System.currentTimeMillis() - start);
    }

    private String downCsvFile(int taskId, String appKey, String apiKey, String day, StringBuilder err) {
        String filePath = "";
        HttpEntity entity = null;
        LOG.info("[Mopub] downCsvFile start, taskId:{}, appKey:{}, apiKey:{}, day:{}", taskId, appKey, apiKey, day);
        long start = System.currentTimeMillis();
        try {
            String url = "https://app.mopub.com/reports/custom/api/download_report?" +
                    "report_key=" + apiKey + "&api_key=" + appKey + "&date=" + day;
            updateReqUrl(jdbcTemplate, taskId, url);
            HttpGet httpGet = new HttpGet(url);
            httpGet.setConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.IGNORE_COOKIES).setProxy(cfg.httpProxy).build());

            HttpResponse response = MyHttpClient.getInstance().execute(httpGet);
            entity = response.getEntity();
            StatusLine sl = response.getStatusLine();
            entity = response.getEntity();
            if (sl.getStatusCode() != 200) {
                err.append(String.format("request report response statusCode:%d,msg:%s", sl.getStatusCode(), entity == null ? "" : EntityUtils.toString(entity)));
                return filePath;
            }

            if (entity == null) {
                err.append("request report response enity is null");
                return filePath;
            }
            if (entity.isStreaming()) {
                try (InputStream instream = entity.getContent()) {
                    if (instream != null) {
                        String path = String.format("/mopub/mopub-%s-%s.csv", day, appKey);
                        File dst = new File(downloadDir, path);
                        File dst_dir = dst.getParentFile();
                        FileUtils.forceMkdir(dst_dir);
                        FileUtils.copyInputStreamToFile(instream, dst);
                        instream.close();
                        filePath = downloadDir + path;
                    }
                } catch (Exception e) {
                    err.append(String.format("download csv error,msg:%s", e.getMessage()));
                }
            }
        } catch (Exception ex) {
            err.append(String.format("downCsvFile error,msg:%s", ex.getMessage()));
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        LOG.info("[Mopub] downCsvFile end, taskId:{}, appKey:{}, apiKey:{}, day:{}, cost:{}", taskId, appKey, apiKey, day, System.currentTimeMillis() - start);
        return filePath;
    }

    private String readCsvFile(String csvFilePath, String day, String appKey, String apiKey) {
        String fileEncoder = "UTF-8";
        BufferedReader in = null;
        String sql_delete = "delete from report_mopub where day=? and app_key=?";
        String error = "";
        LOG.info("[Mopub] readCsvFile start, appKey:{}, apiKey:{}, day:{}", appKey, apiKey, day);
        long start = System.currentTimeMillis();
        try {
            jdbcTemplate.update(sql_delete, day, appKey);
        } catch (Exception e) {
            return String.format("delete report_mopub error, msg:%s", e.getMessage());
        }

        try {
            String insertSql = "INSERT into report_mopub (day,app,app_id,adunit,adunit_id,adunit_format,country," +
                    "device,platform,requests,impressions,clicks,conversions,revenue,ctr,app_key)  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            in = new BufferedReader(new InputStreamReader(new FileInputStream(csvFilePath), fileEncoder));
            String rec;
            int count = 0;
            List<Object[]> lsParm = new ArrayList<>(1000);
            while ((rec = in.readLine()) != null) {
                count++;
                if (count > 1) {
                    if (",".equals(rec.substring(rec.length() - 1)))
                        rec = rec + "0";
                    Object[] arr = rec.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);//双引号内的逗号不分割 双引号外的逗号进行分割
                    Object[] params = new Object[arr.length + 1];
                    for (int i = 0; i < arr.length; i++) {
                        if (i == 0) {
                            params[0] = arr[0].toString();
                            params[arr.length] = appKey;
                        }
                        if (params[i] == null || "".equals(arr[i].toString().replace("\"", "")))
                            params[i] = 0;
                        if (i != 0)
                            params[i] = arr[i].toString().replace("\"", "");
                    }
                    lsParm.add(params);
                    if (lsParm.size() == 1000) {
                        jdbcTemplate.batchUpdate(insertSql, lsParm);
                        lsParm.clear();
                        count = 1;
                    }
                }
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplate.batchUpdate(insertSql, lsParm);
            }
        } catch (IOException e) {
            error = String.format("read csv error, msg:%s", e.getMessage());
        } catch (Exception e) {
            error = String.format("insert report_mopub error, msg:%s", e.getMessage());
        } finally {
            IOUtils.closeQuietly(in);
        }
        LOG.info("[Mopub] readCsvFile end, appKey:{}, apiKey:{}, day:{}, cost:{}", appKey, apiKey, day, System.currentTimeMillis() - start);
        return error;
    }

    private String savePrepareReportData(ReportTask task, String reportDay, String appKey) {
        LOG.info("[Mopub] savePrepareReportData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error;
        try {
            List<Map<String, Object>> instanceInfoList = getInstanceList(task.reportAccountId);
            Map<String, Map<String, Object>> placements = instanceInfoList.stream().collect(Collectors.toMap(m -> MapHelper.getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue));

            // instance's placement_key changed
            Set<Integer> insIds = instanceInfoList.stream().map(o->MapHelper.getInt(o, "instance_id")).collect(Collectors.toSet());
            List<Map<String, Object>> oldInstanceList = getOldInstanceList(insIds);
            if (!oldInstanceList.isEmpty()) {
                placements.putAll(oldInstanceList.stream().collect(Collectors.toMap(m ->
                        MapHelper.getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue)));
            }

            String dataSql = "select day,country,platform,adunit_id data_key,sum(requests) AS api_request,sum(impressions) AS api_impr," +
                    "sum(clicks) AS api_click,sum(revenue) AS revenue " +
                    "  from report_mopub where day=? and app_key=? group by day,country,adunit_id" +
                    " order by adunit_id";

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, appKey);

            if (oriDataList.isEmpty())
                return "data is null";
            error = toAdnetworkLinked(task, appKey, placements, oriDataList);
        } catch (Exception e) {
            error = String.format("savePrepareReportData error, msg:%s", e.getMessage());
        }
        LOG.info("[Mopub] savePrepareReportData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }
}
