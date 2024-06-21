BEGIN TRANSACTION;

-- external table ddl --
DROP TABLE  IF EXISTS "raw_external_fund" CASCADE; --for purposes of this assessment only
CREATE TABLE IF NOT EXISTS "raw_external_fund" (
	"financial_type"	TEXT,
	"symbol"	TEXT,
	"security_name"	TEXT,
	"sedol"	TEXT,
	"price"	REAL,
    "quantity"	REAL,
	"realised_pl"	REAL,
    "market_value"	REAL,
	"fund_name"	TEXT,
	"date"	TEXT,
	"hash_uid"	TEXT,
    "isin" TEXT
);

-- ref table create by changing date format --
-- bond_reference table--
DROP TABLE IF EXISTS "ref_bond_reference";--for purposes of this assessment only
CREATE TABLE IF NOT EXISTS "ref_bond_reference" (
	"security_name"	TEXT,
	"isin"	TEXT,
	"sedol"	TEXT,
	"country"	TEXT,
	"coupon"	REAL,
    "maturity_date"	TEXT,
	"coupon_frequency"	TEXT,
	"sector"	TEXT,
	"currency"	TEXT
);
INSERT INTO "ref_bond_reference" (
select 
	b."SECURITY NAME" security_name,
	b."ISIN" isin,
	b."SEDOL" sedol,
	b."COUNTRY" country,
	b."COUPON" coupon,
	TO_CHAR(TO_DATE(b."MATURITY DATE", 'DD/MM/YYYY'), 'YYYY-MM-DD') maturity_date,
	b."COUPON FREQUENCY" coupon_frequency,
	b."SECTOR" sector,
	b."CURRENCY" currency
from bond_reference b
);

-- bond_price table--
DROP TABLE IF EXISTS "ref_bond_prices";--for purposes of this assessment only
CREATE TABLE IF NOT EXISTS "ref_bond_prices"(
	"datetime" TEXT,
	"isin" TEXT,
	"price" REAL
);
INSERT INTO "ref_bond_prices"(
select 
	b."DATETIME" datetime,
	b."ISIN" isin,
	b."PRICE" price
from bond_prices b
);

-- equity_reference --
DROP TABLE IF EXISTS "ref_equity_reference";--for purposes of this assessment only
CREATE TABLE IF NOT EXISTS "ref_equity_reference" (
	"symbol" TEXT,
	"country" TEXT,
	"security_name" TEXT,
	"sector" TEXT,
	"industry" TEXT,
	"currency" TEXT
);
INSERT INTO "ref_equity_reference"(
select 
	e."SYMBOL" symbol,
	e."COUNTRY" country,
	e."SECURITY NAME" security_name,
	e."SECTOR" sector,
	e."INDUSTRY" industry,
	e."CURRENCY" currency
from equity_reference e
);

-- equity_prices --
DROP TABLE IF EXISTS "ref_equity_prices";--for purposes of this assessment only
CREATE TABLE IF NOT EXISTS "ref_equity_prices"(
	"datetime" TEXT,
	"symbol" TEXT,
	"price" REAL
);

INSERT INTO "ref_equity_prices"(
select 
	TO_CHAR(TO_DATE(e."DATETIME", 'MM/DD/YYYY'), 'YYYY-MM-DD') datetime,
	e."SYMBOL" symbol,
	e."PRICE" price 
from equity_prices e
);

--Q3: view creation for price reconciliation--
DROP VIEW IF EXISTS price_reconciliation;--for purposes of this assessment only
CREATE VIEW price_reconciliation as (
	select 	f.date month_end_date, 
		f.fund_name fund_name, 
		'Equities' as security_type,
		f.security_name instrument,
		f.price fund_price, 
		e.price ref_price,
		f.price - e.price diff_fund_price,
		(f.price - e.price)*100/e.price perc_diff_fund_price
	from raw_external_fund f
	inner join ref_equity_prices e on e.datetime = f.date and f.symbol = e.symbol
	where f.financial_type='Equities'
	
	union all 
		
	select 	f.date  month_end_date,
		f.fund_name, 
		'Bonds' as security_type,
		f.security_name instrument,
		f.price, 
		b.price ,
		f.price - b.price diff_fund_price,
		(f.price - b.price)*100/b.price perc_diff_fund_price
	from raw_external_fund f
	inner join ref_bond_prices b on b.datetime = f.date and b.isin=f.isin
	where f.financial_type='Government Bonds'
);

--Q4: 
--view to get cummulative sum
DROP VIEW IF EXISTS cummulative_pl;--for purposes of this assessment only
CREATE VIEW cummulative_pl as(
with main_table as (
	select date, fund_name, sum(realised_pl) sum_pl 
	from raw_external_fund 
	where financial_type='Equities'
	group by 1,2
), 
	date_ranked_table as(
	select date, 
	fund_name,
	sum_pl,
	rank() over ( partition by fund_name order by date) as date_rank
	from main_table
)
select
t1.date, 
t1.fund_name,
sum(t2.sum_pl) as cumm_sum
from date_ranked_table t1
join date_ranked_table t2 on t1.date_rank >= t2.date_rank and t1.fund_name=t2.fund_name
group by 1,2
order by 2,1
);

--view to get best performing fund per month
DROP VIEW IF EXISTS best_performer;--for purposes of this assessment only
CREATE VIEW best_performer as (
with sum_pl as (
	select date, fund_name, sum(realised_pl) sum_pl 
	from raw_external_fund 
	where financial_type='Equities'
	group by 1,2
),
	ranked_perf as (
	select *, rank() over (partition by date order by sum_pl desc) as ranking
	from sum_pl
	)
	select * from ranked_perf where ranking=1
);

--view to get best performer report based on highest p/l
DROP VIEW IF EXISTS best_performer_report;--for purposes of this assessment only
CREATE VIEW best_performer_report as(
	select b.date, b.fund_name, b.sum_pl, c.cumm_sum
	from best_performer b
	left join cummulative_pl c on b.date = c.date and b.fund_name = c.fund_name
	order by 1,2
	
);


COMMIT;