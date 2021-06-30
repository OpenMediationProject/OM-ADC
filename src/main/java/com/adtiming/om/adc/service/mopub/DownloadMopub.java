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
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;

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
        this.maxTaskCount = 10;
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
        String apiKey = task.adnAppId;
        String reportKey = task.adnApiKey;
        String day = task.day;
        // Set up AdSense Management API client.
        if (StringUtils.isBlank(apiKey) || StringUtils.isBlank(reportKey)) {
            LOG.error("Mopub，appKey or reportKey is null");
            return;
        }
        LOG.info("[Mopub] executeTaskImpl start, apiKey:{}, day:{}", apiKey, day);
        long start = System.currentTimeMillis();
        updateTaskStatus(jdbcTemplate, task.id, 1, "");
        StringBuilder err = new StringBuilder();
        String error;
        task.step = 1;
        String file_path = downCsvFile(task.id, apiKey, reportKey, day, err);
        if (StringUtils.isNotBlank(file_path) && err.length() == 0) {
            task.step = 2;
            error = readCsvFile(file_path, day, apiKey, reportKey);
            if (StringUtils.isBlank(error)) {
                task.step = 3;
                error = savePrepareReportData(task, day, apiKey);
                if (StringUtils.isBlank(error)) {
                    task.step = 4;
                    error = reportLinkedToStat(task, apiKey);
                }
            }
        } else {
            error = err.toString();
        }
        int status = getStatus(error);
        error = convertMsg(error);
        if (task.runCount >= 4 && status != 2) {
            updateAccountException(jdbcTemplate, task, error);
            LOG.error("[Mopub] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
        } else {
            updateTaskStatus(jdbcTemplate, task.id, status, error);
        }
        LOG.info("[Mopub] executeTaskImpl end, appKey:{}, apiKey:{}, day:{}, cost:{}", apiKey, reportKey, day, System.currentTimeMillis() - start);
    }

    private String downCsvFile(int taskId, String apiKey, String reportKey, String day, StringBuilder err) {
        String filePath = "";
        HttpEntity entity = null;
        LOG.info("[Mopub] downCsvFile start, taskId:{}, appKey:{}, apiKey:{}, day:{}", taskId, apiKey, reportKey, day);
        long start = System.currentTimeMillis();
        try {
            String url = "https://app.mopub.com/reports/custom/api/download_report?" +
                    "report_key=" + reportKey + "&api_key=" + apiKey + "&date=" + day;
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
                        String path = String.format("/mopub/mopub-%s-%s.csv", day, apiKey);
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
        LOG.info("[Mopub] downCsvFile end, taskId:{}, appKey:{}, apiKey:{}, day:{}, cost:{}", taskId, apiKey, reportKey, day, System.currentTimeMillis() - start);
        return filePath;
    }

    private String readCsvFile(String csvFilePath, String day, String apiKey, String reportKey) {
        BufferedReader in = null;
        String sql_delete = "delete from report_mopub where day=? and app_key=?";
        String error = "";
        LOG.info("[Mopub] readCsvFile start, appKey:{}, apiKey:{}, day:{}", apiKey, reportKey, day);
        long start = System.currentTimeMillis();
        try {
            jdbcTemplate.update(sql_delete, day, apiKey);
        } catch (Exception e) {
            return String.format("delete report_mopub error, msg:%s", e.getMessage());
        }

        try {
            String sql_insert = "INSERT into report_mopub (day,app,app_id,adunit,adunit_id,adunit_format,country," +
                    "device,platform,sdk_version,adgroup_network_type,requests,fills,impressions,clicks,revenue,app_key) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            in = new BufferedReader(new InputStreamReader(new FileInputStream(csvFilePath), StandardCharsets.UTF_8));
            String rec;
            int count = 0;
            List<Object[]> lsParm = new ArrayList<>(1000);
            //0:SDK Version,1:Adgroup Network Type,2:Day,3:App Name,4:App ID,5:Adunit Name,6:Adunit ID,7:Adunit Format,8:Country,9:Device Model,10:OS,11:Adserver Requests,12:Adserver Attempts,13:Adserver Impressions,14:Adserver Clicks,15:Adserver Revenue,16:CTR,17:Fill Rate (Inventory),18:Fill Rate (Demand),19:Show Rate (Inventory),20:Show Rate (Demand),21:Fills
            List<Integer> needIndexs = Arrays.asList(2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1, 11, 21, 13, 14, 15);
            while ((rec = in.readLine()) != null) {
                count++;
                if (count > 1) {
                    if (",".equals(rec.substring(rec.length() - 1)))
                        rec = rec + "0";
                    String[] arr = rec.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);//双引号内的逗号不分割 双引号外的逗号进行分割
                    Object[] params = new Object[17];
                    params[params.length - 1] = apiKey;
                    for (int i = 0; i < needIndexs.size(); i++) {
                        String val = arr[needIndexs.get(i)].replace("\"", "");
                        if (StringUtils.isNoneBlank(val)) {
                            params[i] = val;
                        } else {
                            params[i] = 0;
                        }
                    }
                    lsParm.add(params);
                    if (lsParm.size() == 1000) {
                        jdbcTemplate.batchUpdate(sql_insert, lsParm);
                        lsParm.clear();
                        count = 1;
                    }
                }
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplate.batchUpdate(sql_insert, lsParm);
            }
        } catch (IOException e) {
            error = String.format("read csv error, msg:%s", e.getMessage());
        } catch (Exception e) {
            error = String.format("insert report_mopub error, msg:%s", e.getMessage());
        } finally {
            IOUtils.closeQuietly(in);
        }
        LOG.info("[Mopub] readCsvFile end, appKey:{}, apiKey:{}, day:{}, cost:{}", apiKey, reportKey, day, System.currentTimeMillis() - start);
        return error;
    }

    private String savePrepareReportData(ReportTask task, String reportDay, String apiKey) {
        LOG.info("[Mopub] savePrepareReportData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error;
        try {
            List<Map<String, Object>> instanceInfoList = getInstanceList(task.reportAccountId);
            if (instanceInfoList.isEmpty()) {
                return "instance is null";
            }
            LocalDate dataDay = LocalDate.parse(task.day, DATEFORMAT_YMD);
            Map<String, Map<String, Object>> placements = new HashMap<>();
            for (Map<String, Object> ins : instanceInfoList) {
                String key = MapHelper.getString(ins, "placement_key");
                putLinkKeyMap(placements, key, ins, dataDay);
            }

            String dataSql = "select day,country,platform,adunit_id data_key,sum(requests) AS api_request,sum(fills) api_filled,sum(impressions) AS api_impr," +
                    "sum(clicks) AS api_click,sum(revenue) AS revenue " +
                    "  from report_mopub where day=? and app_key=? group by day,country,adunit_id,platform" +
                    " order by adunit_id";

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, apiKey);

            if (oriDataList.isEmpty())
                return "data is null";
            error = toAdnetworkLinked(task, apiKey, placements, oriDataList);
        } catch (Exception e) {
            error = String.format("savePrepareReportData error, msg:%s", e.getMessage());
        }
        LOG.info("[Mopub] savePrepareReportData end, taskId:{}, cost:{}", task.id, System.currentTimeMillis() - start);
        return error;
    }
}
