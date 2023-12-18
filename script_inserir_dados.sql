INSERT INTO country (country_id, country)
SELECT country_id, country
FROM tmp_country

INSERT INTO state (state_id, state, country_id)
SELECT state_id,state,country_id
FROM tmp_state

INSERT INTO city (city_id, city, state_id)
SELECT city_id, 
       city, 
	   state_id
FROM tmp_city

INSERT INTO d_month (month_id, action_month)
SELECT month_id, action_month
FROM tmp_d_month

INSERT INTO d_year (year_id, action_year)
SELECT year_id, action_year
FROM tmp_d_year

INSERT INTO d_week (week_id, action_week)
SELECT week_id, action_week
FROM tmp_d_week

INSERT INTO d_weekday (weekday_id, action_weekday)
SELECT weekday_id, action_weekday
FROM tmp_d_weekday

INSERT INTO d_time (time_id, action_timestamp, week_id, month_id, year_id, weekday_id)
SELECT time_id, action_timestamp, week_id, month_id, year_id, weekday_id
FROM tmp_d_time

INSERT INTO customers (customer_id, first_name, last_name, customer_city, country_name, cpf)
SELECT customer_id, first_name, last_name, customer_city, country_name, cpf
FROM tmp_customers

INSERT INTO accounts (account_id, customer_id, created_at, status, account_branch, account_check_digit, account_number)
SELECT account_id, customer_id, created_at, status, account_branch, account_check_digit, account_number
FROM tmp_accounts

INSERT INTO transfer_ins (id, account_id, amount, transaction_requested_at, transaction_completed_at, status)
SELECT DISTINCT *
FROM(
	SELECT 
		id,
		account_id,
		amount,
		transaction_requested_at,
		CASE WHEN transaction_completed_at IS NOT NULL AND transaction_completed_at != 'None' THEN CAST(transaction_completed_at AS BIGINT) ELSE NULL END AS transaction_completed_at,
		status
	FROM tmp_transfer_ins
	)t
	WHERE t.id is not null 
	AND account_id is not null
	AND amount is not null;


INSERT INTO transfer_outs (id, account_id, amount, transaction_requested_at, transaction_completed_at, status)
SELECT DISTINCT *
FROM(
	SELECT id, 
		   account_id, 
		   amount, 
		   transaction_requested_at, 
		   CASE WHEN transaction_completed_at IS NOT NULL AND transaction_completed_at != 'None' THEN CAST(transaction_completed_at AS BIGINT) ELSE NULL END AS transaction_completed_at, 
		   status
	FROM tmp_transfer_outs
)t
WHERE t.id is not null 
AND account_id is not null
AND amount is not null;

INSERT INTO pix_movements (id, account_id, in_or_out, pix_amount, pix_requested_at, pix_completed_at, status)
SELECT DISTINCT *
FROM(
	SELECT id, 
		   account_id, 
		   in_or_out, 
		   pix_amount, 
		   pix_requested_at, 
		   CASE WHEN pix_completed_at IS NOT NULL AND pix_completed_at != 'None' THEN CAST(pix_completed_at AS BIGINT) ELSE NULL END AS pix_completed_at,
		   status
	FROM tmp_pix_movements
)t
WHERE t.id is not null 
AND t.account_id is not null
AND t.pix_amount is not null
