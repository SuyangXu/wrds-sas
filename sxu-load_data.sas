proc sort data = '~/temp1' out=dates; by date1; quit;
proc sort nodupkey data = dates(obs=2); by date1; quit;

* create a list of data from temp1.dat;
%symdel dates;
proc sql noprint;
	SELECT date1
	INTO :Dates separated by ' '
	from dates;
	quit;
%put &dates;


%macro wordcount(list,delimiter);
/* Count the number of words in &LIST, using &DELIMITER;*/
%local count;
%let count=0;
%do %while(%qscan(&list,&count+1,%str(&delimiter)) ne %str());
%let count = %eval(&count+1); 
%end;
&count;
%mend wordcount;





data work.int15m;
  retain label    "00:00:00-00:00:00"
  fmtname  "int15m"
  start 0
  end 0;
do start = "00:00:00"t to "23:45:00"t by 900;
	end = start + 900;
	label = put(start,tod8.0)||"-"||put(end,tod8.0);
	output;
end;
label = "***OTHER***";
output;
format start end tod8.0;
run;
proc format cntlin = work.int15m; run;



/* create 1 min freq for realized volatility */
data work.int1m;
  retain label    "00:00:00-00:00:00"
  fmtname  "int1m"
  start 0
  end 0;
do start = "00:00:00"t to "23:59:00"t by 60;
	end = start + 60;
	label = put(start,tod8.0)||"-"||put(end,tod8.0);
	output;
end;
label = "***OTHER***";
output;
format start end tod8.0;
run;
proc format cntlin = work.int1m; run;









options VALIDVARNAME=ANY;
%let wdcnt = %wordcount(&dates,' ');

%macro tickp;	
	%do i=1 %to &wdcnt;
		%let dt = %scan(&dates,&i,' ');
		
		proc sql noprint;
			SELECT TAQsymbol
			INTO :firms separated by '","'
			from '~/temp1'
			where date1 = input("&dt",yymmdd8.);
			quit;
		%let firms= "&firms";
		%put &firms;
			
		data trades; set taq.ct_&dt;
			where Symbol in (&firms) and 
			price>0 and size>0 and corr=0 and 
			cond in ("","@","@F","@E","F","E") and G127=0 and 
			time between '09:30:00't and '16:00:00't;
	
			/*rounding time to sample*/
			int15min = put(time, int15m.);
			int1min = put(time, int1m.);
		run;
		
		data nbbo; 
			set taqmsec.nbbom_&dt (keep = DATE TIME_M SYM_ROOT BEST_BID BEST_ASK);
			where SYM_ROOT in (&firms) and 
			TIME_M between '09:30:00't and '16:00:00't;
			by SYM_ROOT TIME_M;
			
			/*rounding time to sample*/
			int15min = put(TIME_M, int15m.);
			int1min = put(TIME_M, int1m.);
			
			/*find bid-ask spread at each row*/
			delta_ask = BEST_ASK - lag(BEST_ASK);
			baspd = BEST_ASK - BEST_BID;
			avgP = (BEST_BID + BEST_ASK)/2;
		run;
	%end;
%mend;
%tickp;