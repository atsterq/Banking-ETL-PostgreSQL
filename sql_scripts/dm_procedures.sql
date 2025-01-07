-- витринa оборотов по лицевым счетам
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
    -- удаляем старые записи за указанную дату
    delete from dm.dm_account_turnover_f where on_date = i_ondate;

    -- обрабатываем каждое уникальное account_rk
    for record in
        select distinct credit_account_rk as account_rk
        from ds.ft_posting_f
        where oper_date = i_ondate
    loop
        -- получаем сумму по кредиту и дебету
        select coalesce(sum(credit_amount), 0), coalesce(sum(debet_amount), 0)
        into v_credit_amount, v_debet_amount
        from ds.ft_posting_f
        where oper_date = i_ondate and 
              (credit_account_rk = record.account_rk or debet_account_rk = record.account_rk);

        -- получаем курс обмена на дату
        select coalesce(reduced_cource, 1) into v_exchange_rate
        from ds.md_exchange_rate_d
        where data_actual_date = i_ondate and currency_rk = record.account_rk;

        -- конвертируем суммы в рубли
        v_credit_amount_rub := v_credit_amount * v_exchange_rate;
        v_debet_amount_rub := v_debet_amount * v_exchange_rate;

        -- вставляем данные в витрину оборотов
        insert into dm.dm_account_turnover_f (
            on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub
        ) values (
            i_ondate, record.account_rk, v_credit_amount, v_credit_amount_rub, v_debet_amount, v_debet_amount_rub
        );
    end loop;

    -- логирование завершения расчета
    raise notice 'turnover calculation completed for %', i_ondate;
end;
$$;


-- процедура заполнения витрины остатков по лицевым счетам
create or replace procedure ds.fill_account_balance_f(i_ondate date)
language plpgsql
as $$
declare
    v_balance_out numeric(23, 8);
    v_balance_out_rub numeric(23, 8);
    v_credit_amount numeric(23, 8);
    v_debet_amount numeric(23, 8);
    v_previous_balance numeric(23, 8);
    v_previous_balance_rub numeric(23, 8);
    v_exchange_rate numeric(23, 8);
	record record; 
begin
    -- удаляем старые записи за указанную дату
    delete from dm.dm_account_balance_f where on_date = i_ondate;

    -- обрабатываем каждый счет
    for record in
        select account_rk, char_type, currency_rk
        from ds.md_account_d
        where data_actual_date <= i_ondate and (data_actual_end_date is null or data_actual_end_date >= i_ondate)
    loop
        -- получаем предыдущий остаток по счету
        select coalesce(balance_out, 0), coalesce(balance_out_rub, 0)
        into v_previous_balance, v_previous_balance_rub
        from dm.dm_account_balance_f
        where account_rk = record.account_rk and on_date = (i_ondate - interval '1 day');

        -- получаем обороты по дебету и кредиту
        select coalesce(sum(debet_amount), 0), coalesce(sum(credit_amount), 0)
        into v_debet_amount, v_credit_amount
        from dm.dm_account_turnover_f
        where account_rk = record.account_rk and on_date = i_ondate;

        -- рассчитываем баланс для активных и пассивных счетов
        if record.char_type = 'А' then
            v_balance_out := v_previous_balance + v_debet_amount - v_credit_amount;
        else
            v_balance_out := v_previous_balance - v_debet_amount + v_credit_amount;
        end if;

        -- получаем курс для конвертации в рубли
        select coalesce(reduced_cource, 1) into v_exchange_rate
        from ds.md_exchange_rate_d
        where data_actual_date = i_ondate and currency_rk = record.currency_rk;

        -- конвертируем остаток в рубли
        v_balance_out_rub := v_balance_out * v_exchange_rate;

        -- вставляем данные в витрину остатков
        insert into dm.dm_account_balance_f (
            on_date, account_rk, balance_out, balance_out_rub
        ) values (
            i_ondate, record.account_rk, v_balance_out, v_balance_out_rub
        );
    end loop;

    -- логирование завершения расчета
    raise notice 'balance calculation completed for %', i_ondate;
end;
$$;


-- test
do $$
declare
    v_date date := '2018-01-01';
begin
    while v_date <= '2018-01-31' loop
        -- Запуск процедуры для оборотов
        call ds.fill_account_turnover_f(v_date);
        
        -- Запуск процедуры для остатков
        call ds.fill_account_balance_f(v_date);
        
        -- Переход к следующему дню
        v_date := v_date + interval '1 day';
    end loop;
end;
$$;

