package com.hansight.v5.migration;

import com.hansight.v5.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

/**
 * Created by ck on 2018/10/10.
 */
@Service
public class Migration implements Runnable {

    private final static Logger LOG = LoggerFactory.getLogger(Migration.class);

    @Autowired
    @Qualifier("h2JdbcTemplate")
    private JdbcTemplate h2Template;
    @Autowired
    @Qualifier("mysqlJdbcTemplate")
    private JdbcTemplate mysqlTemplate;

    @Autowired
    private RestTemplate restTemplate;

    @Value("${migration.adapt.url}")
    private String adaptUrl;
    @Value("${migration.addrule.url}")
    private String addUrl;
    @Value("${migration.filter.convert.url}")
    private String filterConvertUrl;


    @PostConstruct
    private void init() {
        new Thread(this).start();
    }

    @Override
    public void run() {
        LOG.info("start sae migration process......");

        LOG.info("step 1: handle rule types......");
        // 处理规则分类
        LOG.info("delete old rule types.");
        mysqlTemplate.execute("delete from sae_rule_type");
        List<RuleType> oldRuleTypes = h2Template.query("select id, parent_id, name, id_path, name_path from cep_rule_type" , new ListRTRowMapper());
        LOG.info("migrate rule type count: {}.", oldRuleTypes.size());
        Map<String, String> idMap = new HashMap<>();
        Map<String, String> NameMap = new HashMap<>();
        int failedCnt = oldRuleTypes.size();
        while (true) {
            for (RuleType  rt: oldRuleTypes) {
                try {
                    int exists = mysqlTemplate.queryForObject(String.format("select count(*) from sae_rule_type where id=%d", rt.id), Integer.class);
                    if (exists > 0) continue;
                    if (rt.parentId.equals("1")) {
                        rt.idPath = "/" + rt.id;
                        rt.namePath = "/" + rt.name;
                        idMap.put(String.valueOf(rt.id), rt.idPath);
                        NameMap.put(String.valueOf(rt.id), rt.namePath);
                    } else {
                        if (idMap.get(rt.parentId) == null) {
                            continue;
                        } else {
                            rt.idPath = idMap.get(rt.parentId) + "/" + rt.id;
                            rt.namePath = NameMap.get(rt.parentId) + "/" + rt.name;
                            idMap.put(String.valueOf(rt.id), rt.idPath);
                            NameMap.put(String.valueOf(rt.id), rt.namePath);
                        }
                    }
                    mysqlTemplate.execute(String.format("INSERT INTO sae_rule_type(id, parent_id, name, id_path, name_path) VALUES (%d, '%s', '%s', '%s', '%s')",
                            rt.id, rt.parentId, rt.name, rt.idPath, rt.namePath));
                    LOG.info("add rule type=[{}:{}] success", rt.id, rt.name);
                    failedCnt--;
                } catch (Exception e){
                    LOG.error("add rule type=[{}:{}] failed", rt.id, rt.name);
                    e.printStackTrace();
                }
            }
            if(failedCnt<=0)
                break;
        }

        pauseSeconds(3);

        LOG.info("step 2: handle rules......");
        // 处理规则
        LOG.info("delete old rules.");
        mysqlTemplate.execute("delete from sae_rule");
        List<Rule> odlRules = h2Template.query("select id, rule_name, raw from cep_rule" , new ListRRowMapper());
        failedCnt = odlRules.size();
        LOG.info("migrate rule count: {}.", failedCnt);
        while (true) {
            int loopBefore = failedCnt;
            for (Rule or : odlRules) {
                try {
                    int exists = mysqlTemplate.queryForObject(String .format("select count(*) from sae_rule where id=%d", or.id), Integer.class);
                    if(exists > 0) continue;
                    exists = mysqlTemplate.queryForObject(String .format("select count(*) from sae_rule where rule_name='%s'", or.name), Integer.class);
                    if(exists > 0) continue;
                    // convert rule
                    ResponseEntity<Object> adapter = restTemplate.postForEntity(adaptUrl, or.raw, Object.class);
                    // add rule
                    ResponseEntity responseEntity = restTemplate.postForEntity(addUrl, adapter.getBody(), Object.class);
                    if(!responseEntity.getStatusCode().is2xxSuccessful()) {
                        throw new Exception("add rule failed, statusCode=" + responseEntity.getStatusCode());
                    }
                    Map res = (Map)responseEntity.getBody();
                    int statusCode = Integer.parseInt(res.get("statusCode").toString());
                    if(statusCode<0) {
                        throw new Exception("add rule failed: " + res.get("messages"));
                    }
                    // update id
                    mysqlTemplate.update(String.format("update sae_rule set id=%d where rule_name='%s'", or.id, or.name));
                    LOG.info("add rule={} success. failedCnt={}", or.name, --failedCnt);

                } catch (Exception e) {
                    LOG.error("add rule={} failed, raw={}, error={}", or.name, or.raw, e);
                }
            }
            if(loopBefore == failedCnt){
                break;
            }
            if(failedCnt <= 0){
                break;
            }
        }

        pauseSeconds(3);

        // 处理模板 -- 应该不需要升级
        /*mysqlTemplate.execute("delete from sae_template");
        List<Rule> odlRuleTemplates = h2Template.query("select id, name, raw from cep_template" , new ListRRowMapper());
        for (Rule or : odlRuleTemplates) {
            try {
                restTemplate.postForEntity(addUrl, or.raw, Object.class);
                int ok = mysqlTemplate.update(String.format("update sae_template set id=%d where name='%s'", or.id, or.name));
                if(ok > 0){
                    LOG.info("add template={} success", or.name);
                }
            }catch (Exception e){
            }
        }*/

        LOG.info("step 3: handle analysis tasks......");
        // 历史任务
        List<AnalysisTask> analysisTasks = h2Template.query("select * from cep_analysis_task" , new ListATRowMapper());
        LOG.info("migrate analysis task count: {}.", analysisTasks.size());
        for (AnalysisTask at: analysisTasks) {
            try {
                while (true) {
                    int exists = mysqlTemplate.queryForObject(String.format("select count(*) from sae_analysis_task where name='%s'", at.name), Integer.class);
                    if (exists > 0)
                        at.name = at.name + "_(1)";
                    else
                        break;
                }
                mysqlTemplate.execute(String.format("INSERT INTO sae_analysis_task(name,description,begin_time,end_time,cron,relative_rules,status,progress,running_time,create_time) VALUES ('%s', '%s', %d, %d, '%s', '%s', %d, %d, '%s', '%s')",
                        at.name, at.description, at.beginTime, at.endTime, at.cron, at.relativeRules, at.status, at.progress, at.runningTime, at.createTime));
                if(at.startTime != null) {
                    mysqlTemplate.execute(String.format("update sae_analysis_task set start_time='%s' where name='%s'", at.startTime, at.name));
                }
                if(at.userId != null) {
                    mysqlTemplate.execute(String.format("update sae_analysis_task set user_id='%s' where name='%s'", at.userId, at.name));
                }
                if(at.notifier != null) {
                    mysqlTemplate.execute(String.format("update sae_analysis_task set notifier='%s' where name='%s'", at.notifier, at.name));
                }
                LOG.info("add analysis task={} success", at.name);
            }catch (Exception e){
                LOG.error("add analysis task={} failed", at.name);
                e.printStackTrace();
            }
        }

        pauseSeconds(3);

        LOG.info("step 4: handle alg jobs......");
        // 算法模块db filter及special_config升级
        List<AlgJob> algJobs = mysqlTemplate.query("select id, special_cfg, query_string from alg_job" , new ListAlgRowMapper());
        LOG.info("migrate alg job count: {}.", algJobs.size());
        for (AlgJob algJob: algJobs) {
            try {
                if (algJob.specialCfg != null && !algJob.specialCfg.isEmpty()) {
                    Map m = JsonUtil.parseObject(algJob.specialCfg, HashMap.class);
                    if (m != null && m.containsKey("hight_bound")) {
                        m.put("high_bound", m.get("hight_bound"));
                        m.remove("hight_bound");
                    }
                    algJob.specialCfg = JsonUtil.toJsonStr(m);
                }
                if (algJob.queryString != null && !algJob.queryString.isEmpty()) {
                    String filter = restTemplate.postForObject(filterConvertUrl, algJob.queryString, String.class);
                    algJob.queryString = filter;
                }
                mysqlTemplate.execute(String.format("update alg_job set special_cfg='%s', query_string='%s' where id=%d", algJob.specialCfg, algJob.queryString, algJob.id));
                LOG.info("update alg job={} success", algJob.id);
            } catch (Exception e) {
                LOG.info("update alg job={} failed", algJob.id);
                e.printStackTrace();
            }
        }

        LOG.info("migration finish");
        System.exit(0);
    }


    class Rule {
        String name;
        int id;
        String raw;
    }

    class RuleType {
        int id;
        String parentId;
        String name;
        String idPath;
        String namePath;
    }

    class AnalysisTask {
        int id;
        String name;
        long beginTime;
        long endTime;
        Timestamp runningTime;
        int status;
        Timestamp startTime;
        int progress;
        String cron;
        String relativeRules;
        String notifier;
        String description;
        String userId;
        Timestamp createTime;
        Timestamp updateTime;
    }

    class AlgJob {
        long id;
        String specialCfg;
        String queryString;
    }

    class ListRRowMapper implements RowMapper<Rule> {
        @Override
        public Rule mapRow(ResultSet rs, int rowNum) throws SQLException {
            Rule rule = new Rule();
            rule.id = rs.getInt("id");
            rule.name = rs.getString("rule_name");
            rule.raw = rs.getString("raw");
            return rule;
        }
    }

    class ListRTRowMapper implements RowMapper<RuleType> {
        @Override
        public RuleType mapRow(ResultSet rs, int rowNum) throws SQLException {
            RuleType ruleType = new RuleType();
            ruleType.id = rs.getInt("id");
            ruleType.name = rs.getString("name");
            ruleType.parentId = rs.getString("parent_id");
            ruleType.idPath = rs.getString("id_path");
            ruleType.namePath = rs.getString("name_path");
            return ruleType;
        }
    }

    class ListATRowMapper implements RowMapper<AnalysisTask> {
        @Override
        public AnalysisTask mapRow(ResultSet rs, int rowNum) throws SQLException {

            AnalysisTask analysisTask = new AnalysisTask();
            analysisTask.id = rs.getInt("id");
            analysisTask.name = rs.getString("name");
            analysisTask.description = rs.getString("description");
            analysisTask.beginTime = rs.getTimestamp("begin_time").getTime();
            analysisTask.endTime = rs.getTimestamp("end_time").getTime();
            analysisTask.cron = rs.getString("cron");
            analysisTask.relativeRules = rs.getString("relative_rules");
            analysisTask.status = rs.getInt("status");
            analysisTask.startTime = rs.getTimestamp("start_time");
            analysisTask.progress = rs.getInt("progress");
            analysisTask.runningTime = rs.getTimestamp("running_time");
            analysisTask.userId = rs.getString("user_id");
            analysisTask.createTime = rs.getTimestamp("create_time");
            analysisTask.updateTime = rs.getTimestamp("update_time");
            analysisTask.notifier = rs.getString("notifier");
            return analysisTask;
        }
    }

    class ListAlgRowMapper implements RowMapper<AlgJob> {
        @Override
        public AlgJob mapRow(ResultSet rs, int rowNum) throws SQLException {
            AlgJob algJob = new AlgJob();
            algJob.id = rs.getLong("id");
            algJob.specialCfg = rs.getString("special_cfg");
            algJob.queryString = rs.getString("query_string");
            return algJob;
        }
    }

    private void pauseSeconds(int seconds){
        try {
            Thread.sleep(seconds * 1000);
        } catch (Exception e) {
        }
    }
}
