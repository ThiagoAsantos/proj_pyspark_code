SELECT DISTINCT ON (account_number,action_month) 
       action_month AS month,
       account_number AS account,
       SUM(transfer_ins) as "Total Transfer ins",
	   SUM(transfer_outs) as "Total Transfer out",
	   SUM(account_monthly_balance) as "Account Monthly Balance"
FROM(
	WITH transfer_ins_cte AS (
		SELECT A.account_id,
			   B.transaction_requested_at,
			   B.transaction_completed_at,
			   B.amount,
		       C.time_id,
		       C.action_timestamp,
		       E.action_month,
		       D.action_year
		FROM public.accounts A
		LEFT JOIN public.transfer_ins B 
		ON A.account_id = B.account_id
		JOIN public.d_time C 
		ON B.transaction_requested_at = C.time_id 
		AND B.transaction_completed_at = C.time_id
		JOIN public.d_year D
		ON C.year_id = D.year_id
		JOIN public.d_month E
		ON C.month_id = E.month_id
	),
	transfer_outs_cte AS (
		SELECT A.account_id,
			   B.transaction_requested_at,
			   B.transaction_completed_at,
			   B.amount,
		       C.time_id,
		       C.action_timestamp,
		       E.action_month,
		       D.action_year
		FROM public.accounts A
		LEFT JOIN public.transfer_outs B 
		ON A.account_id = B.account_id
		JOIN public.d_time C 
		ON B.transaction_requested_at = C.time_id 
		AND B.transaction_completed_at = C.time_id
		JOIN public.d_year D
		ON C.year_id = D.year_id
		JOIN public.d_month E
		ON C.month_id = E.month_id
	)
	SELECT account_number,
		   transaction_requested_at,
		   transaction_completed_at,
	       time_id,
	       action_timestamp,
		   transfer_ins,
		   transfer_outs,
		   (transfer_ins - transfer_outs) AS account_monthly_balance,
	       action_month,
		   action_year
	FROM(
		SELECT
			A.account_number,
		    transfer_ins_cte.time_id,
		    transfer_ins_cte.action_timestamp,
			COALESCE(transfer_ins_cte.transaction_requested_at, transfer_outs_cte.transaction_requested_at) AS transaction_requested_at,
			COALESCE(transfer_ins_cte.transaction_completed_at, transfer_outs_cte.transaction_completed_at) AS transaction_completed_at,
			COALESCE(transfer_ins_cte.amount, 0) AS transfer_ins,
			COALESCE(transfer_outs_cte.amount, 0) AS transfer_outs,
		   	transfer_ins_cte.action_month,
		    transfer_ins_cte.action_year
		FROM public.accounts A
		LEFT JOIN transfer_ins_cte 
		ON A.account_id = transfer_ins_cte.account_id
		LEFT JOIN transfer_outs_cte 
		ON A.account_id = transfer_outs_cte.account_id
		UNION
		SELECT
			A.account_number,
		    transfer_outs_cte.time_id,
		    transfer_outs_cte.action_timestamp,
			COALESCE(transfer_ins_cte.transaction_requested_at, transfer_outs_cte.transaction_requested_at) AS transaction_requested_at,
			COALESCE(transfer_ins_cte.transaction_completed_at, transfer_outs_cte.transaction_completed_at) AS transaction_completed_at,
			COALESCE(transfer_ins_cte.amount, 0) AS transfer_ins,
			COALESCE(transfer_outs_cte.amount, 0) AS transfer_outs,
		   	transfer_outs_cte.action_month,
		    transfer_outs_cte.action_year
		FROM public.accounts A
		LEFT JOIN transfer_ins_cte 
		ON A.account_id = transfer_ins_cte.account_id
		LEFT JOIN transfer_outs_cte 
		ON A.account_id = transfer_outs_cte.account_id
	)T
	UNION ALL
	SELECT
		A.account_number,
		B.pix_requested_at,
		B.pix_completed_at,
	    C.time_id,
	    C.action_timestamp,
		SUM(CASE WHEN B.in_or_out = 'pix_in' THEN pix_amount ELSE 0 END) AS transfer_in,
		SUM(CASE WHEN B.in_or_out = 'pix_out' THEN pix_amount ELSE 0 END) AS transfer_out,
		(SUM(CASE WHEN B.in_or_out = 'pix_in' THEN pix_amount ELSE 0 END) -
		 SUM(CASE WHEN B.in_or_out = 'pix_out' THEN pix_amount ELSE 0 END)) AS account_monthly_balance‬,
	    E.action_month,
		D.action_year
	FROM public.accounts A
	JOIN public.pix_movements B 
	ON A.account_id = B.account_id
	JOIN public.d_time C 
	ON B.pix_requested_at = C.time_id 
	AND B.pix_completed_at = C.time_id
	JOIN public.d_year D
	ON C.year_id = D.year_id
	JOIN public.d_month E
	ON C.month_id = E.month_id
	GROUP BY A.account_number,
			 B.pix_requested_at,
			 B.pix_completed_at,
	         C.time_id,
	         E.action_month,
			 D.action_year
)T WHERE T.action_year = 2020
   GROUP BY T.account_number,
   			T.action_year,
			T.action_month,
			T.action_timestamp
  ORDER BY T.account_number, T.action_month, T.action_timestamp DESC;