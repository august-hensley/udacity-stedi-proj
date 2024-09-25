CREATE EXTERNAL TABLE IF NOT EXISTS `stedidb`.`step_trainer_landing` (
  `sensorreadingtime` bigint,
  `bigintserialnumber` string,
  `stringdinstancefromobject` int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://big-tiddy-goth-girls/step-trainer/landing/'
TBLPROPERTIES ('classification' = 'json');
