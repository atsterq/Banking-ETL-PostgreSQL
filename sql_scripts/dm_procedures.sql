-- процедура заполнения витрины оборотов по лицевым счетам
create or replace procedure ds.fill_account_turnover_f(i_ondate date)
language plpgsql
as $$
declare
    a_credit_amount numeric(23, 8);
    a_credit_amount_rub numeric(23, 8);
    a_debet_amount numeric(23, 8);
    a_debet_amount_rub numeric(23, 8);
    a_exchange_rate numeric(23, 8);
	record record;
begin
    -- удалить старые данные
    delete from dm.dm_account_turnover_f where on_date = i_ondate;

    -- рассчитываем обороты по каждому счету
    for record in
        select distinct credit_account_rk as account_rk
        from ds.ft_posting_f
        where oper_date = i_ondate
    loop
        -- получаем сумму по кредиту для данного счета
        select coalesce(sum(credit_amount), 0)
        into a_credit_amount
        from ds.ft_posting_f
        where oper_date = i_ondate and credit_account_rk = record.account_rk;

        -- получаем сумму по дебету для данного счета
        select coalesce(sum(debet_amount), 0)
        into a_debet_amount
        from ds.ft_posting_f
        where oper_date = i_ondate and debet_account_rk = record.account_rk;

        -- получаем курс рубля для данной даты
        select coalesce(reduced_cource, 1)
        into a_exchange_rate
        from ds.md_exchange_rate_d
        where data_actual_date = i_ondate and currency_rk = record.account_rk;

        -- конвертируем суммы в рубли
        a_credit_amount_rub := a_credit_amount * a_exchange_rate;
        a_debet_amount_rub := a_debet_amount * a_exchange_rate;

        -- вставляем данные в витрину оборотов
        insert into dm.dm_account_turnover_f (
            on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub
        ) values (
            i_ondate, record.account_rk, a_credit_amount, a_credit_amount_rub, a_debet_amount, a_debet_amount_rub
        );
    end loop;
end;
$$;


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

select coalesce(reduced_cource, 1)
        from ds.md_exchange_rate_d
		where '2017-12-31' between data_actual_date and data_actual_end_date;
		
call ds.fill_balance_31_12_2017();