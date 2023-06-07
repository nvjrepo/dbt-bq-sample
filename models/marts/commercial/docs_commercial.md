{% docs is_member_order_splited_bill %}
This column is logically created based on the case that there will be more than 1 order created for a visit of customer since they want to split bill for example sharing, enjoying promotion
The logic to detect involving 
    1.identify whether the previous bill of the bill is in the same day, branch with that bill (using lag function) 
    2.**and** they must come from members, 
    3.**one exception** is that we don't need to verify for the order that is the 1st one.
{% enddocs %}