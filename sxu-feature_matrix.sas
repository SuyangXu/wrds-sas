/*making dictionary of CUSIP and taqSymbol*/
data masterTAQ; set taq.mast_20:; run;
data masterTAQ; set masterTAQ;
	/*if fdate = . then fdate = datef;*/
	cusip8 = substr(cusip,1,8);
	/*drop datef;*/
	run;
	
proc sort data = masterTAQ; by cusip8 descending fdate; quit;
data masterTAQ; set masterTAQ;
	format nameenddt mmddyy8.;
	by cusip8;
	nameenddt = lag(fdate)-1;
	if first.cusip8 then nameenddt = mdy(12,31,2020);
	run;
data masterTAQ; set masterTAQ;
	where cusip ne "           .";
	run;

proc sort nodupkey data = masterTAQ(keep = Symbol CUSIP8 FDATE rename=(CUSIP8 = CUSIP)); 
	by CUSIP; 
run;

data shrout; set crsp.dsf(keep = CUSIP PERMNO DATE SHROUT); by CUSIP; run;

data shrout; 
	merge shrout(in=x) masterTAQ(in=y);
	by CUSIP;
	if x=1 and y=1;
run;

proc sort nodupkey data = shrout; by Symbol DATE; run;









/*load required trades and nbbo from temp1*/
%include '~/load_dat.sas';

proc sort data = nbbo; by SYM_ROOT DATE TIME_M; run;



/* functions begin here */
/* find Turnover*/
proc sql ;
	create table turnover as 
	select a.Symbol, a.DATE, a.int15min, a.SIZE, b.Symbol, b.DATE, b.SHROUT, 
		a.SIZE/b.SHROUT as turnover
	from trades as a, SHROUT as b
	where a.Symbol = b.Symbol and a.DATE = b.DATE;
quit;






/* create data 'effective_baspd' for effective bid ask spread */
proc means data = nbbo mean; /*find mean of avgP*/
	class SYM_ROOT DATE int15min;
	var baspd;
	by SYM_ROOT DATE int15min;
	output out = effbaspd_step1 mean = mean15min;
run;

proc means data = trades mean; /*find mean of trade prices over 15 min interval */
	class Symbol DATE int15min;
	var Price;
	by Symbol DATE int15min;
	output out = effbaspd_step2 mean = meanPrice n = num_trades;
run;

data effective_bdspd; /* result data, find effective bid ask spread */
	merge effbaspd_step1(keep = SYM_ROOT DATE int15min mean15min 
						 rename=(SYM_ROOT = Symbol))  
		  effbaspd_step2(keep = Symbol DATE int15min meanPrice);
	by Symbol DATE int15min;
	eff_baspd = mean15min/meanPrice;
run;


data masterTAQ; set masterTAQ; run;





/* create data 't_rv' for trades volatility */
data t_rv_s1; /* find last trade price in 1 min interval */
	set trades(keep= Symbol DATE Price int1min int15min);
	by Symbol DATE int1min int15min;
	if last.int1min; run;
run;

/* find squared rate of return i.e. delta of prices retrieved above */
data t_rv_s2; 
	set t_rv_s1 nobs=nobs;
	amt_ret = (Price - lag(Price))/lag(Price);
	sq_ret = amt_ret**2;
run;

/* result data: t_rv: sum up squared rate of return for trade volatility */
proc summary data = t_rv_s2 sum; 
	class Symbol DATE int15min;
	var sq_ret;
	by Symbol DATE;
	output out = t_rv sum = tRV;
run;







/* create data 'nbbo_rv' for nbbo volatility */
data nbbo_rv_s1; /* find last avgP in 1 min interval */
	set nbbo(keep= SYM_ROOT DATE avgP int1min int15min);
	by SYM_ROOT DATE int1min int15min;
	if last.int1min; run;
run;

/* find squared rate of return i.e. delta of prices retrieved above */
data nbbo_rv_s2;
	set nbbo_rv_s1 nobs=nobs;
	amt_ret = (avgP - lag(avgP))/lag(avgP);
	sq_ret = amt_ret**2;
run;

/* result data: nbbo_rv: sum up squared rate of return for nbbo volatility */
proc summary data = nbbo_rv_s2 sum;
	class SYM_ROOT DATE int15min;
	var sq_ret;
	by SYM_ROOT DATE;
	output out = nbbo_rv sum = nbboRV;
run;







/* mkt condition: what are HFTraders doing? trades over quotes */
proc means data = nbbo n; /* count number of nonzeros in delta_ask: denominator */
	class int15min;
	where delta_ask ne 0;
	var delta_ask;
	by SYM_ROOT DATE int15min;
	output out = spd_delta n = num_nonzero;
run;

/* count number of trades in delta_ask: denominator */
proc summary data = trades n; 
	class int15min;
	var size;
	by Symbol DATE int15min;
	output out = tt_trades n = num_trades;
run;

/* result data: ToverQ */
data ToverQ;
	merge tt_trades(keep=Symbol DATE int15min num_trades) 
		  spd_delta(keep=SYM_ROOT DATE int15min num_nonzero rename=(SYM_ROOT=Symbol));
	by Symbol DATE int15min;
	t_over_delta = num_trades/num_nonzero;
run;




