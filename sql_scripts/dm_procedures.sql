-- процедура заполнения витрины оборотов по лицевым счетам
create or replace procedure ds.fill_account_turnover_f(i_ondate date)
language plpgsql
as $$
declare
    v_credit_amount numeric(23, 8);
    v_credit_amount_rub numeric(23, 8);
    v_debet_amount numeric(23, 8);
    v_debet_amount_rub numeric(23, 8);
    v_exchange_rate numeric(23, 8);
	record record;
begin
    -- Удаляем старые данные за указанную дату
    delete from dm.dm_account_turnover_f where on_date = i_ondate;

    -- Рассчитываем обороты по каждому счету
    for record in
        select distinct credit_account_rk as account_rk
        from ds.ft_posting_f
        where oper_date = i_ondate
    loop
        -- Получаем сумму по кредиту для данного счета
        select coalesce(sum(credit_amount), 0)
        into v_credit_amount
        from ds.ft_posting_f
        where oper_date = i_ondate and credit_account_rk = record.account_rk;

        -- Получаем сумму по дебету для данного счета
        select coalesce(sum(debet_amount), 0)
        into v_debet_amount
        from ds.ft_posting_f
        where oper_date = i_ondate and debet_account_rk = record.account_rk;

        -- Получаем курс рубля для данной даты
        select coalesce(reduced_cource, 1)
        into v_exchange_rate
        from ds.md_exchange_rate_d
        where data_actual_date = i_ondate and currency_rk = record.account_rk;

        -- Конвертируем суммы в рубли
        v_credit_amount_rub := v_credit_amount * v_exchange_rate;
        v_debet_amount_rub := v_debet_amount * v_exchange_rate;

        -- Вставляем данные в витрину оборотов
        insert into dm.dm_account_turnover_f (
            on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub
        ) values (
            i_ondate, record.account_rk, v_credit_amount, v_credit_amount_rub, v_debet_amount, v_debet_amount_rub
        );
    end loop;

    -- Логирование завершения расчета
    raise notice 'Turnover calculation completed for %', i_ondate;
end;
$$;


-- заполнить баланс за 31.12.2017
create or replace procedure ds.fill_balance_31_12_2017()
language plpgsql
as $$
declare
    v_balance_out numeric(23, 8);
    v_balance_out_rub numeric(23, 8);
    v_exchange_rate numeric(23, 8);
	record record;
begin
    -- Для 31.12.2017: заполняем остатки из DS.FT_BALANCE_F
    for record in
        select account_rk, balance_out, currency_rk
        from ds.ft_balance_f
        where on_date = '2017-12-31'
    loop
        -- Получаем курс на 31.12.2017
        select coalesce(reduced_cource, 1) into v_exchange_rate
        from ds.md_exchange_rate_d
		where '2017-12-31' between data_actual_date and data_actual_end_date
			and currency_rk = record.currency_rk;

        -- Конвертируем остаток в рубли
        v_balance_out_rub := record.balance_out * v_exchange_rate;

        -- Вставляем данные в витрину остатков
        insert into dm.dm_account_balance_f (
            on_date, account_rk, balance_out, balance_out_rub
        ) values (
            '2017-12-31', record.account_rk, record.balance_out, v_balance_out_rub
        );
    end loop;
end;
$$;

select coalesce(reduced_cource, 1)
        from ds.md_exchange_rate_d
		where '2017-12-31' between data_actual_date and data_actual_end_date;
		
call ds.fill_balance_31_12_2017();