-- Build daily fraud report and append new events into sql_project.rep_fraud
-- Safe for reruns: unique index + ON CONFLICT DO NOTHING

CREATE TABLE IF NOT EXISTS sql_project.rep_fraud (
    event_dt TIMESTAMP NOT NULL,
    passport VARCHAR(32) NOT NULL,
    fio VARCHAR(255) NOT NULL,
    phone VARCHAR(32),
    event_type VARCHAR(255) NOT NULL,
    report_dt TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_rep_fraud_event
    ON sql_project.rep_fraud (event_dt, passport, event_type);

WITH tx AS (
    SELECT
        t.trans_id,
        t.trans_date,
        t.card_num,
        t.oper_type,
        t.amt,
        t.oper_result,
        t.terminal,
        tm.terminal_city,
        a.valid_to,
        cl.passport_num,
        cl.passport_valid_to,
        CONCAT_WS(' ', cl.last_name, cl.first_name, cl.patronymic) AS fio,
        cl.phone
    FROM sql_project.dwh_fact_transactions t
    JOIN sql_project.dwh_dim_cards c
        ON c.card_num = t.card_num
    JOIN sql_project.dwh_dim_accounts a
        ON a.account_num = c.account_num
    JOIN sql_project.dwh_dim_clients cl
        ON cl.client_id = a.client
    LEFT JOIN sql_project.dwh_dim_terminals tm
        ON tm.terminal_id = t.terminal
),
fraud_passport AS (
    SELECT
        tx.trans_date AS event_dt,
        tx.passport_num AS passport,
        tx.fio,
        tx.phone,
        'Операция по просроченному или заблокированному паспорту'::VARCHAR(255) AS event_type
    FROM tx
    WHERE
        (tx.passport_valid_to IS NOT NULL AND tx.trans_date::DATE > tx.passport_valid_to)
        OR EXISTS (
            SELECT 1
            FROM sql_project.dwh_fact_passport_blacklist pb
            WHERE pb.passport_num = tx.passport_num
              AND pb.entry_dt <= tx.trans_date::DATE
        )
),
fraud_contract AS (
    SELECT
        tx.trans_date AS event_dt,
        tx.passport_num AS passport,
        tx.fio,
        tx.phone,
        'Операция при недействующем договоре'::VARCHAR(255) AS event_type
    FROM tx
    WHERE tx.valid_to IS NOT NULL
      AND tx.trans_date::DATE > tx.valid_to
),
fraud_city AS (
    SELECT
        s.trans_date AS event_dt,
        s.passport_num AS passport,
        s.fio,
        s.phone,
        'Операции в разных городах в течение 1 часа'::VARCHAR(255) AS event_type
    FROM (
        SELECT
            tx.*,
            LAG(tx.trans_date) OVER (PARTITION BY tx.card_num ORDER BY tx.trans_date) AS prev_dt,
            LAG(tx.terminal_city) OVER (PARTITION BY tx.card_num ORDER BY tx.trans_date) AS prev_city
        FROM tx
        WHERE tx.oper_result = 'SUCCESS'
    ) s
    WHERE s.prev_dt IS NOT NULL
      AND s.prev_city IS NOT NULL
      AND s.terminal_city IS NOT NULL
      AND s.terminal_city <> s.prev_city
      AND s.trans_date - s.prev_dt <= INTERVAL '1 hour'
),
fraud_amount AS (
    SELECT
        s.trans_date AS event_dt,
        s.passport_num AS passport,
        s.fio,
        s.phone,
        'Попытка подбора суммы (цепочка за 20 минут)'::VARCHAR(255) AS event_type
    FROM (
        SELECT
            tx.*,
            LAG(tx.trans_date, 1) OVER (PARTITION BY tx.card_num ORDER BY tx.trans_date) AS dt_1,
            LAG(tx.trans_date, 2) OVER (PARTITION BY tx.card_num ORDER BY tx.trans_date) AS dt_2,
            LAG(tx.trans_date, 3) OVER (PARTITION BY tx.card_num ORDER BY tx.trans_date) AS dt_3,
            LAG(tx.amt, 1) OVER (PARTITION BY tx.card_num ORDER BY tx.trans_date) AS amt_1,
            LAG(tx.amt, 2) OVER (PARTITION BY tx.card_num ORDER BY tx.trans_date) AS amt_2,
            LAG(tx.amt, 3) OVER (PARTITION BY tx.card_num ORDER BY tx.trans_date) AS amt_3,
            LAG(tx.oper_result, 1) OVER (PARTITION BY tx.card_num ORDER BY tx.trans_date) AS res_1,
            LAG(tx.oper_result, 2) OVER (PARTITION BY tx.card_num ORDER BY tx.trans_date) AS res_2,
            LAG(tx.oper_result, 3) OVER (PARTITION BY tx.card_num ORDER BY tx.trans_date) AS res_3
        FROM tx
    ) s
    WHERE s.oper_result = 'SUCCESS'
      AND s.res_1 = 'REJECT'
      AND s.res_2 = 'REJECT'
      AND s.res_3 = 'REJECT'
      AND s.amt < s.amt_1
      AND s.amt_1 < s.amt_2
      AND s.amt_2 < s.amt_3
      AND s.dt_3 IS NOT NULL
      AND s.trans_date - s.dt_3 <= INTERVAL '20 minutes'
),
all_events AS (
    SELECT * FROM fraud_passport
    UNION ALL
    SELECT * FROM fraud_contract
    UNION ALL
    SELECT * FROM fraud_city
    UNION ALL
    SELECT * FROM fraud_amount
)
INSERT INTO sql_project.rep_fraud (
    event_dt,
    passport,
    fio,
    phone,
    event_type,
    report_dt
)
SELECT
    e.event_dt,
    e.passport,
    e.fio,
    e.phone,
    e.event_type,
    NOW() AS report_dt
FROM all_events e
ON CONFLICT (event_dt, passport, event_type) DO NOTHING;
