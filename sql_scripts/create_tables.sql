-- 1
-- создаем слой детальных данных DS
create schema if not exists ds;

-- создаем таблицы для DS
-- создаем таблицу для DS.FT_BALANCE_F
create table if not exists ds.ft_balance_f (
    on_date date not null,  
    account_rk integer not null,  
    currency_rk integer,  
    balance_out float,  
    primary key (on_date, account_rk)  
);

-- создаем таблицу для DS.FT_POSTING_F
create table if not exists ds.ft_posting_f (
    oper_date date not null,  
    credit_account_rk integer not null,  
    debet_account_rk integer not null,  
    credit_amount float,  
    debet_amount float
);

-- создаем таблицу для DS.MD_ACCOUNT_D
create table if not exists ds.md_account_d (
    data_actual_date date not null,  
    data_actual_end_date date not null, 
    account_rk integer not null,  
    account_number varchar(20) not null, 
    char_type varchar(1) not null, 
    currency_rk integer not null,  
    currency_code varchar(3) not null,  
    primary key (account_rk, data_actual_date)  
);

-- создаем таблицу для DS.MD_CURRENCY_D
create table if not exists ds.md_currency_d (
    currency_rk integer not null, 
    data_actual_date date not null,  
    data_actual_end_date date,  
    currency_code varchar(3),  
    code_iso_char varchar(3),  
    primary key (currency_rk, data_actual_date) 
);

-- создаем таблицу для DS.MD_EXCHANGE_RATE_D
create table if not exists ds.md_exchange_rate_d (
    data_actual_date date not null,  
    data_actual_end_date date,  
    currency_rk integer not null,  
    reduced_cource float,  
    code_iso_num varchar(3),  
    primary key (currency_rk, data_actual_date)  
);

-- создаем таблицу для DS.MD_LEDGER_ACCOUNT_S
create table if not exists ds.md_ledger_account_s (
    chapter char(1),  
    chapter_name varchar(16), 
    section_number integer, 
    section_name varchar(22),  
    subsection_name varchar(21), 
    ledger1_account integer,  
    ledger1_account_name varchar(47), 
    ledger_account integer not null,  
    ledger_account_name varchar(153), 
    characteristic char(1),
    is_resident integer, 
    is_reserve integer,  
    is_reserved integer, 
    is_loan integer, 
    is_reserved_assets integer, 
    is_overdue integer, 
    is_interest integer, 
    pair_account varchar(5), 
    start_date date not null,  
    end_date date,  
    is_rub_only integer, 
    min_term varchar(1),  
    min_term_measure varchar(1), 
    max_term varchar(1), 
    max_term_measure varchar(1),  
    ledger_acc_full_name_translit varchar(1), 
    is_revaluation varchar(1),  
    is_correct varchar(1), 
    primary key (ledger_account, start_date) 
);


-- создаем схему для логов
create schema if not exists logs;

-- создаем таблицу для хранения логов
create table if not exists logs.logs (
    log_id serial primary key,  
    start_time timestamp not null, 
    end_time timestamp,  
    records_count integer, 
    status varchar(50), 
    error_message text  
);


-- 2
-- создаем схему для data mart
create schema if not exists dm;

-- создаем таблицу dm_account_turnover_f
create table if not exists dm.dm_account_turnover_f (
    on_date date not null,  
    account_rk integer not null,  
    credit_amount numeric(23, 8),  
    credit_amount_rub numeric(23, 8),  
    debet_amount numeric(23, 8),  
    debet_amount_rub numeric(23, 8),  
    primary key (on_date, account_rk)
);

-- cоздаем таблицу dm_account_balance_f
create table if not exists dm.dm_account_balance_f (
    on_date date not null,  
    account_rk integer not null,  
    balance_out numeric(23, 8),  
    balance_out_rub numeric(23, 8),  
    primary key (on_date, account_rk)
);


-- 3
-- cоздаем таблицу dm_f101_round_f
create table if not exists dm.dm_f101_round_f (
    from_date date,  
    to_date date,  
    chapter char(1),  
    ledger_account char(5),  
    characteristic char(1), 
    balance_in_rub numeric(23, 8),  
    r_balance_in_rub numeric(23, 8),  -- заполнение колонок с приставкой r_ не описано в задании
    balance_in_val numeric(23, 8),  
    r_balance_in_val numeric(23, 8),  
    balance_in_total numeric(23, 8),  
    r_balance_in_total numeric(23, 8),  
    turn_deb_rub numeric(23, 8),  
    r_turn_deb_rub numeric(23, 8),  
    turn_deb_val numeric(23, 8),  
    r_turn_deb_val numeric(23, 8),  
    turn_deb_total numeric(23, 8),  
    r_turn_deb_total numeric(23, 8),  
    turn_cre_rub numeric(23, 8),  
    r_turn_cre_rub numeric(23, 8),  
    turn_cre_val numeric(23, 8),  
    r_turn_cre_val numeric(23, 8),  
    turn_cre_total numeric(23, 8),  
    r_turn_cre_total numeric(23, 8),  
    balance_out_rub numeric(23, 8),  
    r_balance_out_rub numeric(23, 8),  
    balance_out_val numeric(23, 8),  
    r_balance_out_val numeric(23, 8),  
    balance_out_total numeric(23, 8)
);


-- 4
-- cоздаем таблицу dm_f101_round_f_v2 (копию)
create table if not exists dm.dm_f101_round_f_v2 (
    from_date date,  
    to_date date,  
    chapter char(1),  
    ledger_account char(5),  
    characteristic char(1), 
    balance_in_rub numeric(23, 8),  
    r_balance_in_rub numeric(23, 8),  
    balance_in_val numeric(23, 8),  
    r_balance_in_val numeric(23, 8),  
    balance_in_total numeric(23, 8),  
    r_balance_in_total numeric(23, 8),  
    turn_deb_rub numeric(23, 8),  
    r_turn_deb_rub numeric(23, 8),  
    turn_deb_val numeric(23, 8),  
    r_turn_deb_val numeric(23, 8),  
    turn_deb_total numeric(23, 8),  
    r_turn_deb_total numeric(23, 8),  
    turn_cre_rub numeric(23, 8),  
    r_turn_cre_rub numeric(23, 8),  
    turn_cre_val numeric(23, 8),  
    r_turn_cre_val numeric(23, 8),  
    turn_cre_total numeric(23, 8),  
    r_turn_cre_total numeric(23, 8),  
    balance_out_rub numeric(23, 8),  
    r_balance_out_rub numeric(23, 8),  
    balance_out_val numeric(23, 8),  
    r_balance_out_val numeric(23, 8),  
    balance_out_total numeric(23, 8)
);