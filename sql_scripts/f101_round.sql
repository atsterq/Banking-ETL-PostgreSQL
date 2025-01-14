--процедура расчета 101 формы информации об остатках и оборотах за отчетный период
create or replace procedure  dm.fill_f101_round_f(i_ondate date) -- i_ondate - первый день месяца, следующего за отчетным.
language plpgsql
as $$
declare
	a_start_time timestamp := clock_timestamp();
	a_records_count int4 := 0;
	a_from_date date := i_ondate - interval '1 month' -- начало периода расчета
	a_to_date date := i_ondate - interval '1 day' -- конец периода
begin
    -- удалить старые данные чтобы можно было перезапускать процедуру
    delete from dm.dm_f101_round_f where from_date = a_from_date and to_date = a_to_date;
	
	-- вставляем данные в целевую таблицу
	insert into dm.dm_f101_round_f (
	    from_date date,  
	    to_date date,  
	    chapter char(1),  
	    ledger_account char(5),  
	    characteristic char(1), 
	    balance_in_rub numeric(23, 8),  
	    balance_in_val numeric(23, 8),  
	    balance_in_total numeric(23, 8),  
	    turn_deb_rub numeric(23, 8),  
	    turn_deb_val numeric(23, 8),   
	    turn_deb_total numeric(23, 8),  
	    turn_cre_rub numeric(23, 8),  
	    turn_cre_val numeric(23, 8),  
	    turn_cre_total numeric(23, 8),   
	    balance_out_rub numeric(23, 8),   
	    balance_out_val numeric(23, 8),  
	    balance_out_total numeric(23, 8)
	);

	-- расчёт данных
    with acc_t as (
		select -- таблица счетов
			mlas.chapter -- глава из справочника балансовых счетов
			, mlas.ledger_account -- балансовый счет второго порядка
			, mad.char_type as characteristic -- характеристика счета
		--	, mad.currency_code 
			, mad.account_rk 
		--	, '2018-02-01'::date - interval '1 month' as from_date -- '2018-02-01' заменить на i_ondate
		--	, '2018-02-01'::date - interval '1 day' as to_date
		from
			ds.md_ledger_account_s as mlas 
		join
			ds.md_account_d as mad 
			on mlas.ledger_account = left(mad.account_number, 5)::int -- берем счета второго порядка, т.е. первые 5 символов
		),
		balance_t as (
		select -- таблица всех балансов на начало и конец периода
			dabf.account_rk 
			, dabf.balance_out_rub  as balance_in_total -- сумма остатков в рублях на начало
			, dabf2.balance_out_rub  as balance_out_total -- сумма остатков в рублях на конец
			, mad.currency_code -- для определения типа счета (рублевый или нет)
		from 
			dm.dm_account_balance_f as dabf
		join
			dm.dm_account_balance_f as dabf2
			on dabf.account_rk = dabf2.account_rk 
		join
			ds.md_account_d as mad 
			on dabf.account_rk = mad.account_rk 
		where 
			dabf.on_date = '2018-02-01'::date - interval '1 month' - interval '1 day' -- на день предшествующий первому дню
			and dabf2.on_date = '2018-02-01'::date - interval '1 day' -- последний день расчета
		),
		turn_t as (
		select -- таблица оборотов за период
			datf.account_rk 
			, sum(datf.debet_amount_rub) filter (where mad.currency_code in ('810', '643')) as turn_deb_rub
			, sum(datf.debet_amount_rub) filter (where mad.currency_code not in ('810', '643')) as turn_deb_val
			, sum(datf.debet_amount_rub) as turn_deb_total
			, sum(datf.credit_amount_rub) filter (where mad.currency_code in ('810', '643')) as turn_cre_rub
			, sum(datf.credit_amount_rub) filter (where mad.currency_code not in ('810', '643')) as turn_cre_val
			, sum(datf.credit_amount_rub) as turn_cre_total
		from 
			dm.dm_account_turnover_f as datf 
		join
			ds.md_account_d as mad 
			on datf.account_rk = mad.account_rk 
		where 
			datf.on_date between '2018-02-01'::date - interval '1 month' 
			and '2018-02-01'::date - interval '1 day' -- интервал между первым и последним днем рачетов
		group by datf.account_rk
		)
select 
	acc_t.chapter
	, acc_t.ledger_account
	, acc_t.characteristic
	, sum(balance_t.balance_in_total) filter (where balance_t.currency_code in ('810', '643')) as balance_in_rub
	, sum(balance_t.balance_in_total) filter (where balance_t.currency_code not in ('810', '643')) as balance_in_val
	, sum(balance_t.balance_in_total) as balance_in_total
	, sum(turn_t.turn_deb_rub) as turn_deb_rub
	, sum(turn_t.turn_deb_val) as turn_deb_val
	, sum(turn_t.turn_deb_total) as turn_deb_total
	, sum(turn_t.turn_cre_rub) as turn_cre_rub
	, sum(turn_t.turn_cre_val) as turn_cre_val
	, sum(turn_t.turn_cre_total) as turn_cre_total
	, sum(balance_t.balance_out_total) filter (where balance_t.currency_code in ('810', '643')) as balance_out_rub
	, sum(balance_t.balance_out_total) filter (where balance_t.currency_code not in ('810', '643')) as balance_out_val
	, sum(balance_t.balance_out_total) as balance_out_total
from 
	acc_t
left join 
	balance_t
	on acc_t.account_rk = balance_t.account_rk
left join
	turn_t
	on acc_t.account_rk = turn_t.account_rk
group by 
	acc_t.chapter
	, acc_t.ledger_account
	, acc_t.characteristic;

	-- логирование
	select count(1) into a_records_count from dm.dm_f101_round_f where on_date = i_ondate;

	call logs.logger(
		a_start_time,
		a_records_count,
		'success',
		format('Процедура dm.fill_f101_round_f с аргументом %s выполнена успешно', i_ondate),
		'dm.dm_f101_round_f');

	exception when others then
		call logs.logger(
			a_start_time,
			a_records_count,
			'error',
			sqlerrm,
			'dm.dm_f101_round_f');
end;
$$;


-- удалим записи
truncate dm.dm_f101_round_f ;


-- заполнение dm.dm_account_turnover_f за период 2018-01
do $$
declare
    i_ondate date := '2018-02-01';
	-- i_ondate это первый день месяца, следующего за отчетным. рассчитать за январь, i_ondate = '2018-02-01'
	v_cnt int;
begin
    -- цикл
    while i_ondate <= '2018-01-31' loop
        call ds.fill_account_turnover_f(i_ondate);

		i_ondate := i_ondate + interval '1 day';
    end loop;
end;
$$;


-- проверка
select * from dm.dm_f101_round_f;
select * from logs.logs order by log_id desc ;