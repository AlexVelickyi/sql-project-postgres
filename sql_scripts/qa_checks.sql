-- Quality checks after ETL run

-- 1) Row counts in DWH
SELECT 'dwh_dim_clients' AS table_name, COUNT(*) AS cnt FROM sql_project.dwh_dim_clients
UNION ALL
SELECT 'dwh_dim_accounts', COUNT(*) FROM sql_project.dwh_dim_accounts
UNION ALL
SELECT 'dwh_dim_cards', COUNT(*) FROM sql_project.dwh_dim_cards
UNION ALL
SELECT 'dwh_dim_terminals', COUNT(*) FROM sql_project.dwh_dim_terminals
UNION ALL
SELECT 'dwh_fact_transactions', COUNT(*) FROM sql_project.dwh_fact_transactions
UNION ALL
SELECT 'dwh_fact_passport_blacklist', COUNT(*) FROM sql_project.dwh_fact_passport_blacklist
UNION ALL
SELECT 'rep_fraud', COUNT(*) FROM sql_project.rep_fraud
ORDER BY table_name;

-- 2) Duplicate safety checks
SELECT 'dup_trans_id' AS check_name, COUNT(*) AS dup_cnt
FROM (
    SELECT trans_id
    FROM sql_project.dwh_fact_transactions
    GROUP BY trans_id
    HAVING COUNT(*) > 1
) t
UNION ALL
SELECT 'dup_passport_blacklist', COUNT(*)
FROM (
    SELECT passport_num, entry_dt
    FROM sql_project.dwh_fact_passport_blacklist
    GROUP BY passport_num, entry_dt
    HAVING COUNT(*) > 1
) t
UNION ALL
SELECT 'dup_rep_fraud', COUNT(*)
FROM (
    SELECT event_dt, passport, event_type
    FROM sql_project.rep_fraud
    GROUP BY event_dt, passport, event_type
    HAVING COUNT(*) > 1
) t;

-- 3) Fraud breakdown by type
SELECT event_type, COUNT(*) AS cnt
FROM sql_project.rep_fraud
GROUP BY event_type
ORDER BY cnt DESC, event_type;

-- 4) Latest report batch timestamp
SELECT MAX(report_dt) AS latest_report_dt
FROM sql_project.rep_fraud;

-- 5) META checks
SELECT * FROM sql_project.meta_source_load ORDER BY source_table;
SELECT * FROM sql_project.meta_file_load ORDER BY file_token;
