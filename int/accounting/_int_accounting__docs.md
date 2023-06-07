{% docs int_revenue_expenses %}
Confirmed with the Chief Accountant (CA)
- Sales and COGS are extracted from sale details - `stg_ipos_accounting__sale_details`
- Other expenses are extracted from ledgers - `stg_ipos_accounting__ledgers`
- Weekly COL are extracted from Base Schedule - `fct_weekly_col_index`

Internal management code:
- BOD have an urge to segment expense & revenues to a more detailed category
  than VAS segments => model `stg_vas_internal_code`
- Everytime accountants book an entry, they need to classify the amount to
  proper segment, which is stated in vas <> internal code files (`expense_id`)
- In vas <> internal code files, `expense_id` is mapped to different levels of
  segmentation, with pl1,pl2 and pl3.

Other expenses (cte `expenses`):
- extract account number `641`,`642`,`711`
- expense_id `021`,`022`
- exclude account_contra `911`
- join to vas <> internal ctes for getting pl1,pl2 and pl3
- union to weekly COL, the reason behind that is because COL is finalized
  and book by the accountant at the end of each month, while BOD wants to
  see the amount on a weekly basis => we need to extract weekly COL from 
  the scheduled that is finalized weekly by the HR manager.

Revenue & COGS section (macro `pl_revenue_section`):
- Direct material theory costs
- VAT

HO sections (macro `pl_ho_sections`):
- The CA decided not to book Head office (HO)'s revenues and expenses to ledgers,
due to the fact that they are internal transfer and will be netted off. 
- To compute metrics in HO sections, we need to refer to all 3 sources above,
  and summarize/substract them to HO metrics. We computed 3 metrics as below:
  - Accrual Brand Fund: revenue of HO to marketing activities for outlets  (1% of outlet revenue)
  - Brand Fund: expenses for outlets to collect by HO (1% of outlet revenue)
  - Management Fees: expenses for outlets to collect by HO (8% of outlet revenue)
  - management_consulting revenues: revenue of HO to manage outlets (8% of outlet revenue)
  - The expenses and revenues will be netted off, however, are visible when seeing P/L
    by outlet.

{% enddocs %}

{% docs int_pl_metrics_unioned %}

The model is used to computed P/L metrics from {{ int_revenue_expenses }}, which includes
  - controlable_profit
  - profit_before_taxes
  - cit
  - net_profit
  - ebit
  - ebitda

Since col expense is computed from different sources (refer to the doc {{ int_revenue_expenses }}),
we need to create different loops to capture above metrics for weekly and monthly basis as below:
  - create 2 variables `date_grains` and `date_grains_r`, which includes month and week in list type.
    the order of 2 component `week` and `month` will be placed reversely in `date_grains_r` with `date_grains` 
  - create a for loop with `date_grains` and `date_grains_r` using `zip`. The loop will go run two times,
    with below order:
    - first loop: `date_grains`='month' and `date_grains_r`='week'
    - second loop: `date_grains`='week' and `date_grains_r`='month'
    - The idea behind is `date_grain` is the main granularity level set on the calculation. If, for example
      monthly metrics are computed, the query will set week column (`tran_week`) to null by creating a null
      timestamp column and name the column by the convention: `tran_{{ date_grains_r }}`
    - The logic is also helpful for later segmenting weekly and monthly metrics rows.
  - The above list of metrics will be computed, by month and week. (metric_names+`_metrics` ctes)
  - Create the variable `metric_ctes` to include the list of metrics above and add `_metrics` after, to
    match with ctes used to compute those metrics (cte `unioned`)

  - Create 2 `for` loops, to loop through `date_grain` and `metric_ctes` to union all metrics cte, with
    different granularity levels
  - Final cte is to:
    - add missing columns for union into the original `int_revenue_expenses`
    - union to `int_revenue_expenses`
    - here is how the `date_grains_r` variable is helpful. We created another logic,
      which identifies the rows as `monthly` or `weekly` level by checking whether
      the granularity column is null or not
 
{% enddocs %}