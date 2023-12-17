


-- Drop tables
--DROP TABLE IF EXISTS pix_movements, transfer_outs, transfer_ins, d_time, d_weekday, d_week, d_month, d_year, accounts, customers, city, state, country;

CREATE TABLE d_month (
  month_id INT PRIMARY KEY,
  action_month INT
);

CREATE TABLE d_year (
  year_id INT PRIMARY KEY,
  action_year INT
);

CREATE TABLE d_week (
  week_id INT PRIMARY KEY,
  action_week INT
);

CREATE TABLE d_weekday (
  weekday_id INT PRIMARY KEY,
  action_weekday VARCHAR(128)
);

CREATE TABLE country (
  country_id BIGINT PRIMARY KEY,
  country VARCHAR(128)
);

CREATE TABLE state (
  state_id BIGINT PRIMARY KEY,
  state VARCHAR(128),
  country_id BIGINT,
  FOREIGN KEY (country_id) REFERENCES country (country_id)
);

CREATE TABLE city (
  city_id BIGINT PRIMARY KEY,
  city VARCHAR(256),
  state_id BIGINT,
  FOREIGN KEY (state_id) REFERENCES state (state_id)
);

CREATE TABLE customers (
  customer_id BIGINT PRIMARY KEY,
  first_name VARCHAR(128),
  last_name VARCHAR(128),
  customer_city BIGINT,
  country_name VARCHAR(128),
  cpf BIGINT,
  FOREIGN KEY (customer_city) REFERENCES city (city_id)
);

CREATE TABLE accounts (
  account_id BIGINT PRIMARY KEY,
  customer_id BIGINT,
  created_at TIMESTAMP,
  status VARCHAR(128),
  account_branch VARCHAR(128),
  account_check_digit VARCHAR(128),
  account_number VARCHAR(128),
  FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
);

CREATE TABLE d_time (
  time_id INT PRIMARY KEY,
  action_timestamp TIMESTAMP,
  week_id INT,
  month_id INT,
  year_id INT,
  weekday_id INT,
  FOREIGN KEY (week_id) REFERENCES d_week (week_id),
  FOREIGN KEY (month_id) REFERENCES d_month (month_id),
  FOREIGN KEY (year_id) REFERENCES d_year (year_id),
  FOREIGN KEY (weekday_id) REFERENCES d_weekday (weekday_id)
);

CREATE TABLE transfer_ins (
  id BIGINT PRIMARY KEY,
  account_id BIGINT,
  amount FLOAT,
  transaction_requested_at INT,
  transaction_completed_at INT,
  status VARCHAR(128),
  FOREIGN KEY (account_id) REFERENCES accounts (account_id),
  FOREIGN KEY (transaction_requested_at) REFERENCES d_time (time_id),
  FOREIGN KEY (transaction_completed_at) REFERENCES d_time (time_id)
);

CREATE TABLE transfer_outs (
  id BIGINT PRIMARY KEY,
  account_id BIGINT,
  amount FLOAT,
  transaction_requested_at INT,
  transaction_completed_at INT,
  status VARCHAR(128),
  FOREIGN KEY (account_id) REFERENCES accounts (account_id),
  FOREIGN KEY (transaction_requested_at) REFERENCES d_time (time_id),
  FOREIGN KEY (transaction_completed_at) REFERENCES d_time (time_id)
);

CREATE TABLE pix_movements (
  id BIGINT PRIMARY KEY,
  account_id BIGINT,
  in_or_out VARCHAR(128),
  pix_amount FLOAT,
  pix_requested_at INT,
  pix_completed_at INT,
  status VARCHAR(128),
  FOREIGN KEY (account_id) REFERENCES accounts (account_id),
  FOREIGN KEY (pix_requested_at) REFERENCES d_time (time_id),
  FOREIGN KEY (pix_completed_at) REFERENCES d_time (time_id)
);
