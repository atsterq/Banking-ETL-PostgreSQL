--процедура расчета 101 формы информации об остатках и оборотах за отчетный период
create or replace procedure  dm.fill_f101_round_f(i_ondate date) -- i_ondate - первый день месяца, следующего за отчетным.
language plpgsql
as $$
declare
	a_start_time timestamp := clock_timestamp();
	a_records_count int4 := 0;
	a_from_date date := i_ondate - interval '1 month'; -- начало периода расчета
	a_to_date date := i_ondate - interval '1 day'; -- конец периода
begin
    -- удалить старые данные чтобы можно было перезапускать процедуру
    delete from dm.dm_f101_round_f where from_date = a_from_date and to_date = a_to_date;
	
	-- вставляем данные в целевую таблицу
	insert into dm.dm_f101_round_f (
	    from_date,  
	    to_date,  
	    chapter,  
	    ledger_account,  
	    characteristic, 
	    balance_in_rub,  
	    balance_in_val,  
	    balance_in_total,  
	    turn_deb_rub,  
	    turn_deb_val,   
	    turn_deb_total,  
	    turn_cre_rub,  
	    turn_cre_val,  
	    turn_cre_total,   
	    balance_out_rub,   
	    balance_out_val,  
	    balance_out_total
	)

	-- расчёт данных
    with acc_t as (
		select -- таблица счетов
			mlas.chapter -- глава из справочника балансовых счетов
			, mlas.ledger_account -- балансовый счет второго порядка
			, mad.char_type as characteristic -- характеристика счета
			, mad.account_rk 
		from
			ds.md_ledger_account_s as mlas 
		join
			ds.md_account_d as mad 
			on mlas.ledger_account = left(mad.account_number, 5)::int -- берем счета второго порядка, т.е. первые 5 символов
		),
		balance_t as (
		select -- таблица всех балансов (остатки на начало и конец периода)
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
			dabf.on_date = a_from_date - interval '1 day' -- на день предшествующий первому дню
			and dabf2.on_date = a_to_date -- последний день расчета
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
			datf.on_date between a_from_date and a_to_date -- интервал между первым и последним днем рачетов
		group by datf.account_rk
		)
	select -- финальная выборка
		a_from_date
		, a_to_date
		, acc_t.chapter
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
	select count(1) into a_records_count from dm.dm_f101_round_f where from_date = a_from_date and to_date = a_to_date;

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


-- заполнение dm.dm_account_turnover_f за период 2018-01
-- i_ondate это первый день месяца, следующего за отчетным. рассчитать за январь: i_ondate = '2018-02-01'
call dm.fill_f101_round_f('2018-02-01'::date);


-- проверка
select * from dm.dm_f101_round_f;
select * from logs.logs order by log_id desc ;