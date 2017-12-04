-- QUERY 1

select count(coll_temp.account_id) as total_accounts
from 
	(select account_id, collateral_amount
	from collateral
	where collateral_relation_type = 3
	group by account_id) as coll_temp, 
    
	(select account_id, balance_value
	from balance
	where balance_type = 'Capital'
	group by account_id) as bal_temp
where coll_temp.account_id = bal_temp.account_id
and balance_value > collateral_amount;

-- QUERY 2

select case when total_acc = 0 then null
	else total_bal / total_acc end as total_avg_balance
from 
	(select sum(balance_value) as total_bal
	from balance
	where balance_date <= '2006-01-01'
	and balance_type = 'Capital') as bal_temp,
    
    (select count(account_id) as total_acc
    from account
    where starting_date <= '2006-01-01') as acc_temp;

-- QUERY 3

select count(*) as num_of_collateral
from collateral, customer, balance
where collateral_amount > 1000000
and customer.customer_id = collateral.customer_id
and timestampdiff(year, birth_date, curdate()) > 60
and collateral_relation_type = 2
and balance.account_id = collateral.account_id
and balance_value <= 500000
and balance_type = 'Capital';

-- QUERY 4

select count(*) as num_of_account
from real_estate, collateral, balance
where collateral.collateral_id = real_estate.collateral_id
and collateral_relation_type = 3
and collateral_type = 1
and appreciation_value > 100000
and balance.account_id = collateral.account_id
and balance_value < 10000
and balance_type = 'Interest';
