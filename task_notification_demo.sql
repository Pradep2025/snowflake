-- Set contexts
    USE ROLE RL_DEV_DBA_MMAYO;
    USE DATABASE MM_DEV_DB_MONITORING;
    USE WAREHOUSE DEMO_WH;

-- Begin
    CREATE OR REPLACE SCHEMA DEMO_CLIENT;
    USE SCHEMA DEMO_CLIENT;

-- Create a stored procedure for persisting ACCOUNT_USAGE data
    CREATE OR REPLACE PROCEDURE DEMO_CLIENT.ACCOUNT_USAGE_AUDIT_HISTORY(
        _AUDIT_TABLE VARCHAR
        , _BOOKMARK VARCHAR
    )
    COPY GRANTS
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    $$
        DECLARE
            audit_table_name varchar;
            stage_table_name varchar;
            source_table_name varchar;

        BEGIN

            audit_table_name := 'MM_DEV_DB_MONITORING.DEMO_CLIENT.ACCOUNT_USAGE_' || _AUDIT_TABLE;
            stage_table_name := audit_table_name || '_STAGE';
            source_table_name := 'SNOWFLAKE.ACCOUNT_USAGE.' || _AUDIT_TABLE;

            -- Initialize the audit table

                CREATE TABLE IF NOT EXISTS IDENTIFIER(:audit_table_name)
                AS SELECT
                        SHA2_HEX(OBJECT_CONSTRUCT(*)::VARCHAR) _LOG_ID
                        , OBJECT_CONSTRUCT(*) _PAYLOAD
                        , CURRENT_TIMESTAMP TIME_INSERTED
                   FROM
                        IDENTIFIER(:source_table_name)
                   ORDER BY IDENTIFIER (:_BOOKMARK);

            -- Create the stage table for alerting and incremental loads

                CREATE OR REPLACE TRANSIENT TABLE IDENTIFIER(:stage_table_name)
                AS SELECT
                        SHA2_HEX(OBJECT_CONSTRUCT(*)::VARCHAR) _LOG_ID
                        , OBJECT_CONSTRUCT(*) _PAYLOAD
                        , CURRENT_TIMESTAMP TIME_INSERTED
                   FROM
                        IDENTIFIER(:source_table_name)
                   WHERE 1=1
                        AND _LOG_ID NOT IN (SELECT _LOG_ID FROM IDENTIFIER(:audit_table_name))
                        AND IDENTIFIER(:_BOOKMARK) >= DATEADD('DAYS', -2, CURRENT_TIMESTAMP());


            -- Insert the incremental records into the audit table
                INSERT INTO IDENTIFIER(:audit_table_name)
                    SELECT * FROM IDENTIFIER(:stage_table_name);

        EXCEPTION
            WHEN STATEMENT_ERROR THEN
            RETURN OBJECT_CONSTRUCT('ERROR TYPE' , 'STATEMENT_ERROR'
                                    , 'SQLCODE', sqlcode
                                    , 'SQLERRM', sqlerrm
                                    , 'SQLSTATE', sqlstate
                                    , 'SQLSESSION', CURRENT_SESSION());
        END;
    $$;

    /* Sample calls:

        CALL DEMO_CLIENT.ACCOUNT_USAGE_AUDIT_HISTORY('TASK_HISTORY', 'COMPLETED_TIME');
        CALL DEMO_CLIENT.ACCOUNT_USAGE_AUDIT_HISTORY('COPY_HISTORY', 'LAST_LOAD_TIME');

    */

    SHOW TABLES IN SCHEMA DEMO_CLIENT;

    SELECT * FROM DEMO_CLIENT.ACCOUNT_USAGE_TASK_HISTORY;

-- Create an alerting SP on the TASK_STAGING data
    CREATE OR REPLACE PROCEDURE DEMO_CLIENT.EMAIL_ALERT_ON_TASK_ERROR(_TASK_NAME VARCHAR, _ERROR_VALUE VARCHAR)
    COPY GRANTS
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS
    $$
        DECLARE
            c_get_failed_rs CURSOR FOR (
                SELECT _PAYLOAD:DATABASE_NAME::VARCHAR DATABASE_NAME
                    , _PAYLOAD:SCHEMA_NAME::VARCHAR SCHEMA_NAME
                    , _PAYLOAD:NAME::VARCHAR TASK_NAME
                    , _PAYLOAD:STATE::VARCHAR TASK_STATE
                    , _PAYLOAD:SCHEDULED_TIME::TIMESTAMP TASK_SCHEDULED_TIME
                    , REPLACE(_PAYLOAD::VARCHAR, ',"', '\r\n,"') TASK_DETAILS
                    , PARSE_JSON(_PAYLOAD::VARCHAR) TASK_JSON
                FROM
                    MM_DEV_DB_MONITORING.DEMO_CLIENT.ACCOUNT_USAGE_TASK_HISTORY_STAGE
                WHERE 1=1
                    AND TASK_NAME = ?
                    AND TASK_STATE = ?
                ORDER BY TASK_SCHEDULED_TIME DESC
                    LIMIT 1
                );

        BEGIN

        -- Get the errors
        OPEN c_get_failed_rs USING(_TASK_NAME, _ERROR_VALUE);

        for r in c_get_failed_rs do

            let _DATABASE_NAME varchar := r.DATABASE_NAME;
            let _TASK_DETAILS varchar := r.TASK_DETAILS;
            let _SCHEMA_NAME varchar := r.SCHEMA_NAME;
            let _TASK_NAME varchar := r.TASK_NAME;

            CALL SYSTEM$SEND_EMAIL (
                'MM_DBA_EMAIL_INT'
                    , 'mark.mayo@snowflake.com'
                    , 'Task error name ' || :_DATABASE_NAME || '.' || :_SCHEMA_NAME || '.' || :_TASK_NAME
                    , :_TASK_DETAILS);

        end for;

        EXCEPTION
            WHEN STATEMENT_ERROR THEN
            RETURN OBJECT_CONSTRUCT('ERROR TYPE' , 'STATEMENT_ERROR'
                                    , 'SQLCODE', sqlcode
                                    , 'SQLERRM', sqlerrm
                                    , 'SQLSTATE', sqlstate
                                    , 'SQLSESSION', CURRENT_SESSION());

        END;
    $$;

    -- Note have to refresh the data
    CALL DEMO_CLIENT.ACCOUNT_USAGE_AUDIT_HISTORY('TASK_HISTORY', 'COMPLETED_TIME');

    -- Check if there's any data to report
      SELECT _PAYLOAD:DATABASE_NAME::VARCHAR DATABASE_NAME
            , _PAYLOAD:SCHEMA_NAME::VARCHAR SCHEMA_NAME
            , _PAYLOAD:NAME::VARCHAR TASK_NAME
            , _PAYLOAD:STATE::VARCHAR TASK_STATE
            , _PAYLOAD:SCHEDULED_TIME::TIMESTAMP TASK_SCHEDULED_TIME
            , REPLACE(_PAYLOAD::VARCHAR, ',"', '\r\n,"') TASK_DETAILS
            , PARSE_JSON(_PAYLOAD::VARCHAR) TASK_JSON
        FROM
            MM_DEV_DB_MONITORING.DEMO_CLIENT.ACCOUNT_USAGE_TASK_HISTORY_STAGE
        WHERE 1=1
            AND TASK_NAME = 'REPLICATE_TASK'
            AND TASK_STATE = 'FAILED'
        ORDER BY TASK_SCHEDULED_TIME DESC
            LIMIT 1;

    -- Call the email function
    CALL DEMO_CLIENT.EMAIL_ALERT_ON_TASK_ERROR('REPLICATE_TASK', 'FAILED');
   

CREATE NOTIFICATION INTEGRATION IF NOT EXISTS MM_DBA_EMAIL_INT
        TYPE=EMAIL
        ENABLED=TRUE
        ALLOWED_RECIPIENTS=('mark.mayo@snowflake.com');


    -- Create a monitoring task
        CREATE OR REPLACE TASK MM_DEV_DB_MONITORING.DEMO_CLIENT.TASK_AUDIT_ACCOUNT_USAGE_SP WAREHOUSE = WH_DCR_MMAYO
        SCHEDULE = '5 MINUTE'
            AS CALL DEMO_CLIENT.ACCOUNT_USAGE_AUDIT_HISTORY('TASK_HISTORY', 'COMPLETED_TIME');

        ALTER TASK MM_DEV_DB_MONITORING.DEMO_CLIENT.TASK_AUDIT_ACCOUNT_USAGE_SP SUSPEND;

    -- Create some alerts
        -- Add email dependency for REPLICATE_TASK
        CREATE OR REPLACE TASK MM_DEV_DB_MONITORING.DEMO_CLIENT.TASK_EMAIL_ALERT_ON_TASK_ERROR_REPLICATE_TASK WAREHOUSE = WH_DCR_MMAYO
        AFTER MM_DEV_DB_MONITORING.DEMO_CLIENT.TASK_AUDIT_ACCOUNT_USAGE_SP
            AS CALL DEMO_CLIENT.EMAIL_ALERT_ON_TASK_ERROR('REPLICATE_TASK', 'FAILED');

        -- Add a separate dependency for REFRESH_CLONE_DB
        CREATE OR REPLACE TASK MM_DEV_DB_MONITORING.DEMO_CLIENT.TASK_EMAIL_ALERT_ON_TASK_ERROR_REFRESH_CLONE_DB WAREHOUSE = WH_DCR_MMAYO
        AFTER MM_DEV_DB_MONITORING.DEMO_CLIENT.TASK_AUDIT_ACCOUNT_USAGE_SP
            AS CALL DEMO_CLIENT.EMAIL_ALERT_ON_TASK_ERROR('REFRESH_CLONE_DB', 'FAILED');


        ALTER TASK MM_DEV_DB_MONITORING.DEMO_CLIENT.TASK_EMAIL_ALERT_ON_TASK_ERROR_REPLICATE_TASK RESUME;
        ALTER TASK MM_DEV_DB_MONITORING.DEMO_CLIENT.TASK_EMAIL_ALERT_ON_TASK_ERROR_REFRESH_CLONE_DB RESUME;
        ALTER TASK MM_DEV_DB_MONITORING.DEMO_CLIENT.TASK_AUDIT_ACCOUNT_USAGE_SP RESUME;
