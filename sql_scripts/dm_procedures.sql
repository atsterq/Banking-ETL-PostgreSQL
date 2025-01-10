-- 1.2
-- процедура заполнения витрины оборотов по лицевым счетам
create or replace procedure ds.fill_account_turnover_f(i_ondate date)
language plpgsql
as $$
begin
    -- удалить старые данные
    delete from dm.dm_account_turnover_f where on_date = i_ondate;
	
	-- вставляем данные
	insert into dm.dm_account_turnover_f (
		on_date, account_rk, credit_amount, credit_amount_rub, 
		debet_amount, debet_amount_rub)

	-- расчёт данных
    -- Если по счету не было проводок в дату расчету, то он не должен попадать в витрину в эту дату.
	select 
		i_ondate as on_date,
		coalesce(balance_t.c_account_rk , balance_t.d_account_rk) as account_rk 
		, balance_t.credit_amount 
		, balance_t.credit_amount * coalesce (merd.reduced_cource, 1) as credit_amount_rub
		, balance_t.debet_amount
		, balance_t.debet_amount * coalesce (merd.reduced_cource, 1) as debet_amount_rub
	from ( -- таблица курсов за дату
		select 
			reduced_cource, currency_rk 
		from 
			ds.md_exchange_rate_d
		where -- актуальный курс
			data_actual_date = i_ondate) as merd
	right join ( -- таблица балансов дебета и кредита
		select 
			credit_t.account_rk as c_account_rk 
			, debet_t.account_rk as d_account_rk
			, coalesce(credit_t.currency_rk, debet_t.currency_rk) as currency_rk 
			, credit_t.credit_amount
			, debet_t.debet_amount
		from
			(select -- таблица кредита
			    distinct fbf.account_rk, -- идентификатор счета, по которому были проводки в дату расчету
				fbf.currency_rk
				, SUM(fpf.credit_amount) as credit_amount --сумма проводок за дату расчета
			from
				ds.ft_balance_f as fbf -- 114 счетов
			join
				ds.ft_posting_f as fpf -- с 2018-01-09
				on fbf.account_rk = fpf.credit_account_rk -- где счет участвовал как счет по кредиту
			where 
				fpf.oper_date = i_ondate -- подставить переданную дату (i_ondate)
			group by
				fbf.account_rk, fbf.currency_rk
			) 
				as credit_t
		full join ( -- таблица дебета
			select 
			    distinct fbf.account_rk,
				fbf.currency_rk
				, SUM(fpf.debet_amount) as debet_amount
			from
				ds.ft_balance_f as fbf 
			join
				ds.ft_posting_f as fpf 
				on fbf.account_rk = fpf.debet_account_rk -- где счет участвовал как счет по debet
			where 
				fpf.oper_date = i_ondate 
			group by
				fbf.account_rk, fbf.currency_rk
			) 
				as debet_t	
		on credit_t.account_rk = debet_t.account_rk)
		as balance_t
	on merd.currency_rk = balance_t.currency_rk;
end;
$$;


-- проверка за один день
call ds.fill_account_turnover_f('2018-01-09');


-- заполнение dm.dm_account_turnover_f за период 2018-01
do $$
declare
    i_ondate date := '2018-01-01';
begin
    -- цикл
    while i_ondate <= '2018-01-31' loop
        call ds.fill_account_turnover_f(i_ondate);

        i_ondate := i_ondate + interval '1 day';
    end loop;
end;
$$;


select * from dm.dm_account_turnover_f as datf limit 100;
select count(1) from dm.dm_account_turnover_f as datf ;


-----------------------------------
-- Так как остатки за день считаются на основе остатков за предыдущий день, 
-- вам необходимо заполнить витрину DM.DM_ACCOUNT_BALANCE_F за 31.12.2017 данными из DS.FT_BALANCE_F.
truncate dm.dm_account_balance_f ;
-- Заполним баланс за 31.12.2017
insert into dm.dm_account_balance_f (
	on_date, account_rk, balance_out, balance_out_rub)
select
	'2017-12-31' as on_date , account_rk, balance_out
	, balance_out * coalesce (reduced_cource, 1) as balance_out_rub
from
	ds.ft_balance_f as fbf 
left join
	ds.md_exchange_rate_d as merd
	on merd.currency_rk = fbf.currency_rk
		and merd.data_actual_date = '2017-12-31';

	
-- процедура заполнения витрины остатков по лицевым счетам.
create or replace procedure ds.fill_account_balance_f(i_ondate date)
language plpgsql
as $$
begin
    -- удалить старые данные
    delete from dm.account_balance_f where on_date = i_ondate;
	
	-- вставляем данные
	insert into dm.account_balance_f (
	on_date, account_rk, balance_out, balance_out_rub)


end;
$$;


-- проверка за один день
call ds.fill_account_turnover_f('2018-01-09');


-- заполнение dm.dm_account_turnover_f за период 2018-01
do $$
declare
    i_ondate date := '2018-01-01';
begin
    -- цикл
    while i_ondate <= '2018-01-31' loop
        call ds.fill_account_turnover_f(i_ondate);

        i_ondate := i_ondate + interval '1 day';
    end loop;
end;
$$;


select * from dm.dm_account_turnover_f as datf limit 100;
select count(1) from dm.dm_account_turnover_f as datf ;
	
	
	
	
	
	
	
	