--datamart creation
drop view if exists shipping_datamart;
create view shipping_datamart as
(select si.shippingid,
        si.vendorid,
        st.transfer_type,
        date_part('day', age(ss.shipping_end_fact_datetime,ss.shipping_start_fact_datetime))::int as full_day_at_shipping,
        case
        	when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then 1
       	else 0
        end as is_delay,
        case
        	when ss.status = 'finished' then 1
       	else 0
        end as is_shipping_finished,
        case
        	when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then 
       	    date_part('day', age(ss.shipping_end_fact_datetime, si.shipping_plan_datetime))::int
       	else 0
        end as delay_day_at_shipping,
        si.payment_amount,
        (si.payment_amount*(scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate))::numeric(14,5) as vat,
        (si.payment_amount*agreement_commission)::numeric(14,5) as profit 
from public.shipping_info as si
left outer join public.shipping_status ss on si.shippingid = ss.shippingid
left outer join public.shipping_transfer st on si.transfer_type_id = st.id
left outer join public.shipping_country_rates scr on si.shipping_country_id = scr.id
left outer join public.shipping_agreement sa on si.agreementid = sa.agreementid); 

