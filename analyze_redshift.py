import psycopg2 as pg
import logging
import logging.config


import etl_cfg



logger = logging.getLogger(__name__)
logging.config.dictConfig(etl_cfg.LOGGING_CONFIG)
rs_conn = pg.connect(etl_cfg.REDSHIFT_DB_URL)


def get_table_list():
    logger.info("Getting list of tables from DB")
    sql = """SELECT "schema",
           "table"
	   FROM SVV_TABLE_INFO
	   WHERE ("schema","table") NOT IN (SELECT schema_name, table_name FROM dwh.etl_vacuum_exclusion)
       AND ("schema" NOT IN ('stg')  and  "schema" not like 'temp%')
       AND ("table" not like '%_test%' and "table" not like '%_old%')
	   ORDER BY CASE WHEN "schema" = 'dwh'THEN 9
		             WHEN "schema" = 'dim' THEN 8
		      ELSE 6 END DESC,
		   size DESC"""
    rs_cursor = rs_conn.cursor()
    rs_cursor.execute(sql)
    logger.info("Num. tables to be analyzed: %s" % (rs_cursor.rowcount,))

    if rs_cursor.rowcount == 0:
        logger.info("Stats for all tables are up to date.")
        exit(0)
    else:
        list_of_tbls = rs_cursor.fetchall()

    rs_cursor.close()
    return list_of_tbls

def run_analyze():
    table_list = get_table_list()
    num_tables_analyzed = 0
    for table in table_list:
        sql = "ANALYZE " + ".".join(table) +"; commit;"
        logger.info("Executing command: "+ sql)
        try:
            rs_cursor = rs_conn.cursor()
            rs_cursor.execute(sql)
            rs_cursor.close()
            num_tables_analyzed += 1
        except:
            logger.warn("Unable to execute: "+sql)

    logger.info("Num. tables analyzed: %s" % (num_tables_analyzed,))


if __name__ == '__main__':
    run_analyze()
