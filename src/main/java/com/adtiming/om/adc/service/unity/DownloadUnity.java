// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.service.unity;

import com.adtiming.om.adc.dto.ReportAdnData;
import com.adtiming.om.adc.dto.ReportTask;
import com.adtiming.om.adc.service.AdnBaseService;
import com.adtiming.om.adc.service.AppConfig;
import com.adtiming.om.adc.util.DateTimeFormat;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class DownloadUnity extends AdnBaseService {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private AppConfig cfg;

    @Value("${download.dir}")
    private String downloadDir;

    @Override
    public void setAdnInfo() {
        this.adnId = 4;
        this.adnName = "Unity";
    }

    @Override
    public void executeTaskImpl(ReportTask o) {
        try {
            downloadData(o);
        } catch (Exception e) {
            LOG.error("[{}] executeTaskImpl error, taskId:{}", this.adnName, o.id, e);
        }
    }

    private void downloadData(ReportTask task) {
        String appKey = task.adnApiKey;
        String day = task.day;
        String hour = task.hour < 10 ? "0" + task.hour : String.valueOf(task.hour);
        // Set up AdSense Management API client.
        if (StringUtils.isBlank(appKey)) {
            //executeTaskIds.remove(task.id);
            LOG.error("[Unity] appKey is null");
            return;
        }
        LOG.info("[Unity] download start,appKey:{}, day:{}, hour:{}", appKey, day, hour);
        long start = System.currentTimeMillis();
        try {
            updateTaskStatus(jdbcTemplate, task.id, 1, "");

            String startDate = day + "T" + hour + ":00:00.000Z";
            String endDate = day + "T" + hour + ":59:59.999Z";
            String url = "https://gameads-admin.applifier.com/stats/monetization-api?apikey=" + appKey
                    + "&splitBy=source,zone,country&fields=adrequests,available,offers,started,views,revenue" +
                    "&start=" + startDate + "&end=" + endDate + "&scale=hour";
            updateReqUrl(jdbcTemplate, task.id, url);
            StringBuilder sb = new StringBuilder();
            String file_path = DownCsvFile(url, day, hour, appKey, sb);
            String error;
            if (StringUtils.isNotBlank(file_path)) {
                error = ReadCsvFile(file_path, day, hour, appKey);
                if (StringUtils.isBlank(error)) {
                    error = savePrepareReportData(task, day, task.hour, appKey);
                    if (StringUtils.isBlank(error)) {
                        error = reportLinkedToStat(task, appKey);
                    }
                }
            } else {
                error = sb.toString();
            }
            int status = StringUtils.isBlank(error) || "data is null".equals(error) ? 2 : 3;
            if (task.runCount > 5 && status != 2) {
                LOG.error("[Unity] executeTaskImpl error,run count:{},taskId:{},msg:{}", task.runCount + 1, task.id, error);
            }
            updateTaskStatus(jdbcTemplate, task.id, status, error);
        } catch (Exception e) {
            updateTaskStatus(jdbcTemplate, task.id, 3, e.getMessage());
        }
        //executeTaskIds.remove(task.id);
        LOG.info("[Unity] download end,appKey:{}, day:{}, hour:{}, cost:{}", appKey, day, hour, System.currentTimeMillis() - start);
    }

    private String DownCsvFile(String url, String day, String hour, String appKey, StringBuilder sb) {
        String file_path = "";
        HttpEntity entity = null;
        try {
            HttpGet httpGet = new HttpGet(url);
            LOG.info("[Unity] request url:{}, day:{}, hour:{}, appKey:{}", url, day, hour, appKey);
            httpGet.setConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.IGNORE_COOKIES).setProxy(cfg.httpProxy).build());
            //发送Post,并返回一个HttpResponse对象
            HttpResponse response = MyHttpClient.getInstance().execute(httpGet);
            entity = response.getEntity();
            StatusLine sl = response.getStatusLine();
            if (sl.getStatusCode() != 200) {//如果状态码为200,就是正常返回
                LOG.info("[Unity] response status code is not 200,statusCode:{},appKey:{},day:{},hour:{}",
                        sl.getStatusCode(), appKey, day, hour);
                sb.append(String.format("response status code is not 200,statusCode:%d", sl.getStatusCode()));
                return file_path;
            }
            if (entity == null) {
                LOG.info("[Unity] response data is null,appKey:{},day:{},hour:{}", appKey, day, hour);
                sb.append("response data is null");
                return file_path;
            }
            if (entity.isStreaming()) {
                try (InputStream instream = entity.getContent()) {
                    if (instream != null) {
                        String path = String.format("/unity/unity-%s-%s-%s.csv", day, hour, appKey);
                        File dst = new File(downloadDir, path);
                        File dst_dir = dst.getParentFile();
                        FileUtils.forceMkdir(dst_dir);
                        FileUtils.copyInputStreamToFile(instream, dst);
                        instream.close();
                        file_path = downloadDir + path;
                    }
                } catch (Exception e) {
                    sb.append(String.format("Download csv error,error:%s", e.getMessage()));
                    LOG.info("[Unity] Download csv error,appKey:{},day:{},hour:{}", appKey, day, hour, e);
                    return file_path;
                }
                LOG.info("[Unity] Download csv finished,appKey:{},day:{},hour:{}", appKey, day, hour);
                return file_path;
            } else {
                sb.append("response entity is not a stream");
            }
            LOG.info("[Unity] data is null,appKey:{},day:{},hour:{}", appKey, day, hour);
        } catch (Exception ex) {
            sb.append(String.format("Download csv error,error:%s", ex.getMessage()));
            LOG.info("[Unity] Download csv error,appKey:{},day:{},hour:{}", appKey, day, hour, ex);
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
        return file_path;
    }

    /**
     * 读取CSV文件
     *
     * @param csvFilePath 文件路径
     */
    private String ReadCsvFile(String csvFilePath, String day, String hour, String appKey) {
        String fileEncoder = "UTF-8";//读取文件编码方式，主要是为了解决中文乱码问题。
        //BufferedReader in = null;
        String sql_delete = "delete from report_unity where day=? and hour=? and app_key=?";
        long start = System.currentTimeMillis();
        LOG.info("[Unity] insert report_unity start,appKey:{},day:{},hour:{}", appKey, day, hour);
        try {
            jdbcTemplate.update(sql_delete, day, hour, appKey);
        } catch (Exception e) {
            //log.error("[Unity] delete report_unity error", e);
            return String.format("delete report_unity error,%s", e.getMessage());
        }

        String error = "";
        int count = 0;
        try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(csvFilePath), fileEncoder))) {
            String sql_insert = "INSERT INTO report_unity (day,source_game_id,source_game_name,source_zone,country," +
                    "country_tier,adrequests,available,offers,started,views,revenue,hour,app_key)  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            String rec;
            boolean have_data = false;
            List<Object[]> lsParm = new ArrayList<>(1000);
            while ((rec = in.readLine()) != null) {
                count++;
                if (count > 1) {
                    have_data = true;
                    Object[] arr = rec.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);//双引号内的逗号不分割 双引号外的逗号进行分割
                    Object[] params = new Object[arr.length + 2];
                    for (int i = 0; i < arr.length; i++) {
                        if (i == 0) {
                            String reportDay;
                            String reportHour;
                            try {
                                LocalDateTime time = LocalDateTime.parse(arr[0].toString().replace("\"", ""), DateTimeFormat.TIME_FORMAT);
                                reportDay = DateTimeFormat.DAY_FORMAT.format(time);
                                reportHour = DateTimeFormat.HOUR_FORMAT.format(time);
                            } catch (Exception e) {
                                reportDay = day;
                                reportHour = hour;

                            }
                            params[0] = reportDay;
                            params[arr.length] = reportHour;
                            params[arr.length + 1] = appKey;
                        }
                        if (params[i] == null || "".equals(arr[i].toString().replace("\"", "")))
                            params[i] = 0;
                        if (i != 0)
                            params[i] = arr[i].toString().replace("\"", "");
                    }
                    lsParm.add(params);
                    if (lsParm.size() == 1000) {
                        jdbcTemplate.batchUpdate(sql_insert, lsParm);
                        lsParm.clear();
                    }
                }
            }
            if (!have_data) { //没有拉取到数据
                LocalDateTime lastDay = LocalDateTime.parse(day + " " + hour + ":00:00", DateTimeFormat.TIME_FORMAT);
                java.time.Duration duration = java.time.Duration.between(lastDay, LocalDateTime.now());
                int hours = (int) duration.toHours();
                if (hours > 23) {
                    LOG.warn("[Unity] Failed to pull data for more than 24 hours,day:{},hour:{},appKey:{}", day, hour, appKey);
                }
                return "data is null";
            }
            if (!lsParm.isEmpty()) {
                jdbcTemplate.batchUpdate(sql_insert, lsParm);
            }
        } catch (Exception e) {
            error = String.format("delete report_unity error,%s", e.getMessage());
        }
        LOG.info("[Unity] insert report_unity finished,appKey:{},day:{},hour:{}, count:{}, cost:{}", appKey, day, hour, count, System.currentTimeMillis() - start);
        return error;
    }

    private String savePrepareReportData(ReportTask task, String reportDay, int reportHour, String appKey) {
        LOG.info("[Unity] savePrepareReportData start, taskId:{}", task.id);
        long start = System.currentTimeMillis();
        String error;
        try {
            String whereSql = String.format("b.api_key='%s'", appKey);
            List<Map<String, Object>> instanceInfoList = getInstanceList(whereSql);

            Map<String, Map<String, Object>> placements = instanceInfoList.stream().collect(Collectors.toMap(m ->
                    MapHelper.getString(m, "adn_app_key") + "_" + MapHelper.getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue));
            placements.putAll(instanceInfoList.stream().collect(Collectors.toMap(m ->
                    MapHelper.getString(m, "adn_app_key") + "_" + MapHelper.getString(m, "adn_app_key"), m -> m, (existingValue, newValue) -> existingValue)));

            // instance's placement_key changed
            Set<Integer> insIds = instanceInfoList.stream().map(o->MapHelper.getInt(o, "instance_id")).collect(Collectors.toSet());
            List<Map<String, Object>> oldInstanceList = getOldInstanceList(insIds);
            if (!oldInstanceList.isEmpty()) {
                placements.putAll(oldInstanceList.stream().collect(Collectors.toMap(m ->
                        MapHelper.getString(m, "adn_app_key") + "_" + MapHelper.getString(m, "placement_key"), m -> m, (existingValue, newValue) -> existingValue)));
            }

            String dataSql = "select day,hour,country,case when source_zone is null or source_zone = '' then concat(source_game_id,'_',source_game_id) else concat(source_game_id,'_',source_zone) end data_key," +
                    "sum(adrequests) api_requests,sum(available) api_filled," +
                    "sum(started) api_impr,sum(views) api_click," +
                    "sum(started) api_video_start,sum(views) api_video_complete," +
                    "sum(revenue) revenue " +
                    " from report_unity" +
                    " where day=? and hour=? and app_key=? " +
                    " group by hour,day,country,source_game_id,source_game_name,source_zone";

            List<ReportAdnData> oriDataList = jdbcTemplate.query(dataSql, ReportAdnData.ROWMAPPER, reportDay, reportHour, appKey);

            if (oriDataList.isEmpty())
                return "data is empty";

            error = toAdnetworkLinked(task, appKey, placements, oriDataList);
        } catch (Exception e) {
            //log.error("[Unity] savePrepareReportData error", e);
            error = String.format("savePrepareReportData error:%s", e.getMessage());
        }
        LOG.info("[Unity] savePrepareReportData end, appKey:{}, day:{}, hour:{}, cost:{}", appKey, reportDay, reportHour, System.currentTimeMillis() - start);
        return error;
    }
}

