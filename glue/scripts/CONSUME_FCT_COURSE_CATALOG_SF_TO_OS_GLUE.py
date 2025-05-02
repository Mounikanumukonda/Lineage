import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
import json
import boto3
from py4j.java_gateway import java_import
from pyspark import SparkContext, conf
from cdp.pyframe.configparser import configparser
from cdp.pyframe import constants
from cdp.pyframe.utils import pythonfuncs as pf
from cdp.pyframe.utils import secretretriever
from cdp.pyframe.utils import pysparkfuncs as psf
from cdp.pyframe.connectors.writers import elasticsearch
import traceback
from pyspark.sql.types import *
from pyspark.sql import Row


# 1. Get command line arguments and create spark, glue context, and spark session

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'config_bucket','config_key', 'environment'])

# Initialize Glue, Spark session, context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Initialize logger
logger =  glueContext.get_logger()
logger.info("Spark and glue session objects are created successfully")

config_bucket = args['config_bucket']
config_key = args['config_key']
# Added for handling environment parmeters
environment = args['environment'].lower()

# 2. Create S3 and snowflake connection objects and get the config object
s3_conn = boto3.client(constants.S3)

try:
    config_obj = configparser.ConfigParser()
    jconfig = config_obj.getjconfigobject(s3_conn,config_bucket,config_key)
    logger.info("Config file is parsed and Json config object is created successfully")

except Exception as e:
    logger.error("Unable to parse s3 config object" + ".\n{}".format(traceback.format_exc()))
    raise e

global_params = config_obj.getglobalparameters(jconfig)
env_params = config_obj.getenvironmentparameters(jconfig)[environment]
job_params =  config_obj.getjobparameters(jconfig)
stage_params = config_obj.getstagesparameters(jconfig)
controlparams = config_obj.getcontrolparameters(jconfig)

snowflake_source_name = global_params['snowflake_source_name']

print("global_params are :", global_params)
print("env_params are :", env_params)
print("job_params are :", job_params)
print("stage_params are :", stage_params)

logger.info("Environment, Job and stages parameters are created successfully")

#---------------------------------------------------------------------------------------------------------------------------------------------

# Stage : load_data_from_sf_to_es params
load_data_from_sf_to_es_sources_params = stage_params[0]['load_data_from_sf_to_es']['sources'][0]
load_data_from_sf_to_es_targets_params = stage_params[0]['load_data_from_sf_to_es']['targets'][0]

#----------------------------------------------------------------------------------------------------------------------------------------------

# Snowflake connection object preparation
snowflake_db_secret_name = env_params['snowflake_db_secret_name']
secretobj = secretretriever.Secret()
secret= json.loads(secretobj.get_secret(snowflake_db_secret_name))

sfuser = secret['username']
sfPassword = secret['password']

java_import(spark._jvm, snowflake_source_name)

spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.disablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())

# Stage 1 : Connection parameters - create_spark_temp_vws_from_sources
sfgenurl = secret['url']
sfgenaccount = secret['account']
sfgenwarehouse = secret['warehouse']
sfstg1database = env_params['database']
sfstg1schema = load_data_from_sf_to_es_sources_params['schema']
sfstg1role = env_params['role']

sfOptionsstg1 = pf.getsnowflakeconobj(sfgenurl,sfgenaccount,sfuser,sfPassword,sfgenwarehouse,sfstg1database,sfstg1schema,sfstg1role)

logger.info("Snowflake connection objects are created successfully")

#-----------------------------------------------------------------------------------------------------------------------------------------------
# Stage 1 : Parse and get load_data_from_sf_to_es_sources_params
# Source params
tmp_vw_query_map = load_data_from_sf_to_es_sources_params['tmp_vw_query_map']
tmp_vw_table_map = load_data_from_sf_to_es_sources_params['tmp_vw_table_map']

# Target params
es_secret = env_params['es_secret']
secretobj = secretretriever.Secret()
es_secret_name = json.loads(secretobj.get_secret(es_secret))
es_net_http_auth_user = es_secret_name['es.net.http.auth.user']
es_net_http_auth_pass = es_secret_name['es.net.http.auth.pass']
#src_cols = load_data_from_sf_to_es_targets_params['source_db_columns']
tgt_cols = load_data_from_sf_to_es_targets_params['target_es_columns']
index_name = load_data_from_sf_to_es_targets_params['name']
es_index = load_data_from_sf_to_es_targets_params['es_index']
target_type = load_data_from_sf_to_es_targets_params['type']
options = env_params['options']
options[global_params['http_auth_user']] = es_net_http_auth_user
options[global_params['http_auth_pass']] = es_net_http_auth_pass
write_mode = load_data_from_sf_to_es_targets_params['write_mode']
write_format = load_data_from_sf_to_es_targets_params['write_format']
tzvalue = load_data_from_sf_to_es_targets_params['tzvalue']
schema_key=load_data_from_sf_to_es_targets_params['schema_key']
# Zero Index fix - start
host = options['opensearch.nodes']
region = load_data_from_sf_to_es_targets_params['region']
es_index_prev = load_data_from_sf_to_es_targets_params['es_index_prev']
es_index_alias = load_data_from_sf_to_es_targets_params['es_index_alias']
# Zero Index fix - start

#-----------------------------------------------------------------------------------------------------------------------------------------------
# Set spark options:
# "set spark.sql.legacy.timeParserPolicy=LEGACY","set spark.sql.ansi.enabled=True","set spark.sql.storeAssignmentPolicy=ANSI"
spark_options = job_params['spark_options']
for option in spark_options:
    spark.sql(option)

#-----------------------------------------------------------------------------------------------------------------------------------------------
# Business logic Implementation:
# Stage 1 - Create spark dataframe of queries in df query map or df table map, cast decimal datatypes, map to elasticsearch column names, and write to elasticsearch
if len(tmp_vw_query_map) > 0:
    for map in tmp_vw_query_map:
        for key,value in map.items():
            sparktmpvwnm = key
            doCache = value[0]
            query = value[1]
            try:
                print("Spark Temporary View name is ", sparktmpvwnm)
                print("Query used to create dataframe is ", query)
                psf.createsparktempviewfromquery(spark,snowflake_source_name,sfOptionsstg1,query,sparktmpvwnm,doCache)
                print("spark temporary view is created {0}".format(sparktmpvwnm))

            except Exception as e:
                logger.error("Unable to create spark temporary view: " + sparktmpvwnm + ".\n{}".format(traceback.format_exc()))
                raise e


if len(tmp_vw_table_map) > 0:
    for map in tmp_vw_table_map:
        for key,value in map.items():
            sparktmpvwnm = key
            doCache = value[0]
            table = value[1]
            try:
                print("Spark Temporary View name is ", sparktmpvwnm)
                print("Table used to create dataframe is ", table)
                psf.createsparktempviewfromtable(spark,snowflake_source_name,sfOptionsstg1,table,sparktmpvwnm,doCache)
                print("spark dataframe is created {0}".format(sparktmpvwnm))

            except Exception as e:
                logger.error("Unable to create spark temporary view: " + sparktmpvwnm + ".\n{}".format(traceback.format_exc()))
                raise e

logger.info("Spark temporary views are created")

training_ref_df = spark.sql("""
SELECT TRAINING_OBJECT_ID,
       TRAINING_TYPE,
       ACTIVE_INACTIVE_FLAG,
       EVENT_NUMBER,
       TRAINING_TITLE,
       TRAINING_HOURS,
       TRAINING_PRICE,
       TRAINING_PROVIDER,
       FIRM_OR_LOCAL,
       DELIVERY_METHOD,
       COURSE_OWNER_FIRMWIDE_LEARNING,
       COURSE_OWNER_LOCAL_OFFICE,
       KLP_FRAMEWORK,
       COURSE_PRIORITY,
       COURSE_LEVEL,
       TARGET_AUDIENCE,
       TRAINING_DESCRIPTION1,
       TRAINING_DESCRIPTION2,
       SOURCE_SYSTEM,
       TRAINING_AVAILABILITY,
       SOURCE_DEEPLINK,
       MARKETING_DESCRIPTION,
       CREATION_DATE,
       UPDATE_DATE,
       IS_LATEST_TRAINING_VERSION,
       REGISTER_AND_LAUNCH_DEEPLINK,
       TRAINING_VERSION_RELATIONSHIP
from VW_TRAINING_REF
""")

training_ref_df_count = training_ref_df.count()											   

training_ref_df = training_ref_df.select(
    [col(name) if 'timestamp' not in colType else date_format(col(name),"yyyy-MM-dd'T'HH:mm:ss'Z'").alias(name) for name, colType in training_ref_df.dtypes]
)

# checks and converts all decimal datatypes to int or double. In elasticsearch, decimal datatype is not supported
# If scale value is zero, cast decimal to int else cast decimal to double
training_ref_df = training_ref_df.select(
    [col(name) if 'decimal' not in colType else col(name).cast(psf.convert_decimal_to_intordouble(colType)) for name, colType in training_ref_df.dtypes]
)

# checks whether the column datatype is date and type cast to string
training_ref_df = training_ref_df.select(
    [col(name) if 'date' not in colType else date_format(to_timestamp(concat(col(name).cast("string"),lit(' 00:00:00')),"yyyy-MM-dd HH:mm:ss"),"yyyy-MM-dd'T'HH:mm:ss'Z'").alias(name) for name, colType in training_ref_df.dtypes]
)

src_cols = training_ref_df.columns
training_ref_tgt_cols = tgt_cols[0]['training_ref_sdf']
training_ref_df = psf.renameCols(training_ref_df, src_cols, training_ref_tgt_cols)
training_ref_df.createOrReplaceTempView("TRAINING_REF_CORE_VW")

traning_taxonomy_tag_df = spark.sql("""
select TRAINING_OBJECT_ID,
       TAG_TYPE_TAG_LINEAGE,
       UPDATE_DATE
from VW_TRAINING_TAXONOMY_TAG
""")

traning_taxonomy_tag_df_count = traning_taxonomy_tag_df.count()															   

traning_taxonomy_tag_df = traning_taxonomy_tag_df.select(
    [col(name) if 'timestamp' not in colType else date_format(col(name),"yyyy-MM-dd'T'HH:mm:ss'Z'").alias(name) for name, colType in traning_taxonomy_tag_df.dtypes]
)

# checks and converts all decimal datatypes to int or double. In elasticsearch, decimal datatype is not supported
# If scale value is zero, cast decimal to int else cast decimal to double
traning_taxonomy_tag_df = traning_taxonomy_tag_df.select(
    [col(name) if 'decimal' not in colType else col(name).cast(psf.convert_decimal_to_intordouble(colType)) for name, colType in traning_taxonomy_tag_df.dtypes]
)

# checks whether the column datatype is date and type cast to string
traning_taxonomy_tag_df = traning_taxonomy_tag_df.select(
    [col(name) if 'date' not in colType else date_format(to_timestamp(concat(col(name).cast("string"),lit(' 00:00:00')),"yyyy-MM-dd HH:mm:ss"),"yyyy-MM-dd'T'HH:mm:ss'Z'").alias(name) for name, colType in traning_taxonomy_tag_df.dtypes]
)

src_cols = traning_taxonomy_tag_df.columns
traning_taxonomy_tgt_cols = tgt_cols[1]['training_taxonomy_tag_sdf']
traning_taxonomy_tag_df = psf.renameCols(traning_taxonomy_tag_df, src_cols, traning_taxonomy_tgt_cols)
traning_taxonomy_tag_df.createOrReplaceTempView("TRAINING_TAXONOMY_TAG_VW")

traning_taxonomy_tag_df = spark.sql("""
select trainingObjectId,
       collect_list(struct(tagTypeTagLineage)) trainingTaxonomies,
       MAX(updateDate) updateDate
from TRAINING_TAXONOMY_TAG_VW
GROUP BY trainingObjectId
""")

traning_taxonomy_tag_df.createOrReplaceTempView("TRAINING_TAXONOMY_TAG_VW")

api_course_catalog_final = spark.sql("""
SELECT TRC.trainingObjectId,
       TRC.trainingType, 
       TRC.activeInactiveFlag,
       TRC.eventNumber,
       TRC.trainingTitle,
       TRC.trainingHours,
       TRC.trainingPrice,
       TRC.trainingProvider,
       TRC.firmOrLocal,
       TRC.deliveryMethod,
       TRC.courseOwnerFirmwideLearning,
       TRC.courseOwnerLocalOffice ,
       TRC.klpFramework,
       TRC.coursePriority,
       TRC.courseLevel,
       TRC.targetAudience,
       TRC.trainingDescription1,
       TRC.trainingDescription2,
       TRC.sourceSystem,
       TRC.trainingAvailability,
       TRC.sourceDeeplink,
       TRC.marketingDescription,
       TRC.createDate,
       TRC.updateDate,
       TRC.isLatestTrainingVersion,
       TRC.registerAndLaunchDeeplink,
       TRC.trainingVersionRelationship,
       TTT.trainingTaxonomies,
       MAX(GREATEST(TRC.updateDate, TTT.updateDate)) over (PARTITION BY TRC.trainingObjectId) lastUpdateDate
FROM TRAINING_REF_CORE_VW TRC
LEFT JOIN TRAINING_TAXONOMY_TAG_VW TTT
    ON TRC.trainingObjectId = TTT.trainingObjectId
"""
)

# Write spark dataframe to elasticsearch in overwrite mode

#esobj = elasticsearch.Elasticsearch()
#esobj.write_sparkdf_to_elasticsearch(api_course_catalog_final,write_format,write_mode,es_index,options)

if api_course_catalog_final.count() > 0 and traning_taxonomy_tag_df_count > 0 and training_ref_df_count > 0:
    # Write spark dataframe to OpenSearch
    osobj = elasticsearch.Elasticsearch()
    os = osobj.get_opensearch_object(os_region=region,os_host=host,http_auth_username=es_net_http_auth_user,http_auth_password=es_net_http_auth_pass)
													
    osobj.preprocess(os,es_index_alias,es_index,es_index_prev,config_bucket,schema_key)
    logger.info(f"preprocess completed for index {es_index}:switched alias {es_index_alias} to prev {es_index_prev}, refreshed index data to prev, and removed alias")
    osobj.write_sparkdf_to_opensearch(api_course_catalog_final,write_format,write_mode,es_index,options)
    osobj.postprocess(os,es_index_alias,es_index,es_index_prev)
    logger.info(f"postprocess completed for index {es_index}: switched alias {es_index_alias} to index {es_index}, and removed alias {es_index_alias} from prev {es_index_prev}")
						  
    
    logger.info('Opensearch index is loaded successfully')
logger.info('Glue job completed successfully')

job.commit()
