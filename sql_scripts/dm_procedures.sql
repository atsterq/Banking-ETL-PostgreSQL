-- Если по счету не было проводок в дату расчету, то он не должен попадать в витрину в эту дату
-- c 2018-01-01 по 2018-01-08 не было проводок?
select 
	account_rk , balance_t.currency_rk , credit_amount, debet_amount,
--	merd.reduced_cource 
	credit_amount * coalesce (reduced_cource, 1) as credit_amount_rub
from 
	ds.md_exchange_rate_d as merd 
right join (
select 
    fbf.account_rk, -- идентификатор счета, по которому были проводки в дату расчету
	fbf.currency_rk,
	SUM(fpf.credit_amount) as credit_amount, --сумма проводок за дату расчета
	SUM(fpf.debet_amount) as debet_amount
from
	ds.ft_balance_f as fbf -- 114 счетов
join
	ds.ft_posting_f as fpf -- самая ранняя дата 2018-01-09
	on fbf.account_rk = fpf.credit_account_rk -- где счет участвовал как счет по кредиту
where 
	fpf.oper_date = '2018-01-09' -- подставить переданную дату (i_ondate)
group by
	fbf.account_rk, fbf.currency_rk
	) as balance_t
on merd.currency_rk = balance_t.currency_rk;