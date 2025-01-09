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

	-- расчёт
    -- Если по счету не было проводок в дату расчету, то он не должен попадать в витрину в эту дату
	-- c 2018-01-01 по 2018-01-08 не было проводок? - да
	select 
		i_ondate as on_date,
		account_rk, 
		credit_amount, 
		credit_amount * coalesce (reduced_cource, 1) as credit_amount_rub,
		debet_amount,
		debet_amount * coalesce (reduced_cource, 1) as debet_amount_rub
	from (
		select 
			*
		from 
			ds.md_exchange_rate_d
		where -- актуальный курс
			data_actual_date = i_ondate) as merd
	right join (
		select 
		    distinct fbf.account_rk, -- идентификатор счета, по которому были проводки в дату расчету
			fbf.currency_rk,
			SUM(fpf.credit_amount) as credit_amount, --сумма проводок за дату расчета
			SUM(fpf.debet_amount) as debet_amount
		from
			ds.ft_balance_f as fbf -- 114 счетов
		join
			ds.ft_posting_f as fpf -- с 2018-01-09
			on fbf.account_rk = fpf.credit_account_rk -- где счет участвовал как счет по кредиту
		where 
			fpf.oper_date = i_ondate -- подставить переданную дату (i_ondate)
		group by
			fbf.account_rk, fbf.currency_rk
		) as balance_t
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

        -- некст день
        i_ondate := i_ondate + interval '1 day';
    end loop;
end;
$$;












-----------------------------------
-- заполнить баланс за 31.12.2017
create or replace procedure ds.fill_balance_31_12_2017()
language plpgsql
as $$
declare
    a_balance_out numeric(23, 8);
    a_balance_out_rub numeric(23, 8);
    a_exchange_rate numeric(23, 8);
	record record;
begin
    -- для 31.12.2017 заполняем остатки из DS.FT_BALANCE_F
    for record in
        select account_rk, balance_out, currency_rk
        from ds.ft_balance_f
        where on_date = '2017-12-31'
    loop
        -- получаем курс на 31.12.2017
        select coalesce(reduced_cource, 1) into a_exchange_rate
        from ds.md_exchange_rate_d
		where '2017-12-31' between data_actual_date and data_actual_end_date
			and currency_rk = record.currency_rk;

        -- конвертируем остаток в рубли
        a_balance_out_rub := record.balance_out * a_exchange_rate;

        -- вставляем данные в витрину остатков
        insert into dm.dm_account_balance_f (
            on_date, account_rk, balance_out, balance_out_rub
        ) values (
            '2017-12-31', record.account_rk, record.balance_out, a_balance_out_rub
        );
    end loop;
end;
$$;

--select coalesce(reduced_cource, 1)
--        from ds.md_exchange_rate_d
--		where '2017-12-31' between data_actual_date and data_actual_end_date;
		
call ds.fill_balance_31_12_2017();