use eszter;

CREATE TABLE eszter.block_usage (
    created_at DATETIME,
    updated_at DATETIME,
    launch_time DATETIME,
    env VARCHAR(20),
    customer_id INT,
    suite_campaign_id INT,
    campaign_type INT,
    template_id VARCHAR(30),
    template_block_templates_count INT,
    template_blocks_count INT,
    template_unique_blocks_count INT,
    campaign_block_templates_count INT,
    campaign_blocks_count INT,
    campaign_unique_blocks_count INT
);

select count(*) from eszter.block_usage;
select * from eszter.block_usage limit 50;
select count(*) from eszter.block_usage where template_block_templates_count is null;
truncate table eszter.block_usage;

set GLOBAL mysqlx_connect_timeout=120;
set net_read_timeout=360;
set net_write_timeout=360;
set interactive_timeout=120;

show variables like "secure_file_priv";
show variables like "net_read_timeout";
show variables like "net_write_timeout";
show variables like "interactive_timeout";
show variables like "max_allowed_packet";
show variables like "mysqlx_connect_timeout";
show variables like "sql_mode";
show variables like "SQL_SAFE_UPDATES";
SET SQL_SAFE_UPDATES = 0;
SELECT @@GLOBAL.sql_mode;

-- Reference: https://stackoverflow.com/questions/2675323/mysql-load-null-values-from-csv-data
LOAD DATA INFILE 'C:\\Users\\ESZTERKARD\\simplified_template\\block_usage_for_upload.txt'
INTO TABLE eszter.block_usage
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 LINES 
(created_at, updated_at, launch_time, env, customer_id, suite_campaign_id, campaign_type, template_id, template_block_templates_count, template_blocks_count, template_unique_blocks_count, campaign_block_templates_count, campaign_blocks_count, campaign_unique_blocks_count)
set
  template_block_templates_count = NULLIF(template_block_templates_count,''),
  template_blocks_count = NULLIF(template_blocks_count,''),
  template_unique_blocks_count = NULLIF(template_unique_blocks_count,''),
  template_block_templates_count = NULLIF(template_block_templates_count,'')
;
commit;



