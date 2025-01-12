--Отчетная дата – это первый день месяца, следующего за отчетным. 
--То есть, если мы хотим рассчитать отчет за январь 2018 года, то должны передать в процедуру 1 февраля 2018 года.
--target_date = i_ondate - interval '1 month'
-- В отчет должна попасть информация по всем счетам, действующим в отчетном периоде, 
--группировка в отчете идет по балансовым счетам второго порядка
-- group by [:5] счета (DS.MD_ACCOUNT_D.account_number)
--i_ondate - interval '1 month'
--	a_to_date date := i_ondate - interval '1 day'

select -- таблица счетов
	mlas.chapter 
	, mlas.ledger_account
	, mad.char_type as characteristic
--	, mad.currency_code 
--	, mad.account_rk 
	, '2018-02-01'::date - interval '1 month' as from_date -- '2018-02-01' заменить на i_ondate
	, '2018-02-01'::date - interval '1 day' as to_date
from
	ds.md_ledger_account_s as mlas 
join
	ds.md_account_d as mad 
	on mlas.ledger_account = left(mad.account_number, 5)::int -- берем счета второго порядка, т.е. первые 5 символов
;


--процедура расчета 101 формы информации об остатках и оборотах за отчетный период
create or replace procedure  dm.fill_f101_round_f(i_ondate date) -- i_ondate - первый день месяца, следующего за отчетным.
language plpgsql
as $$
declare
	a_start_time timestamp := clock_timestamp();
	a_records_count int4 := 0;
	a_from_date date := i_ondate - interval '1 month'
	a_to_date date := i_ondate - interval '1 day'
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