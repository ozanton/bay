--shipping_country_rates
insert into public.shipping_country_rates 
(shipping_country, shipping_country_base_rate)
select shipping_country,
       shipping_country_base_rate
from (select distinct shipping_country, shipping_country_base_rate from public.shipping) as sub;

--shipping_agreement
insert into public.shipping_agreement
(agreementid, agreement_number, agreement_rate, agreement_commission)
select distinct description[1]:: int as agreementid,
       description[2]::text as agreement_number,
       description[3]::numeric(14,2) as agreement_rate,
       description[4]:: numeric(14,2) as agreement_comission
from (select regexp_split_to_array(vendor_agreement_description, E'\\:+') as description
      from public.shipping) as vendor_agreement_information
order by agreementid;

--shipping_transfer
insert into public.shipping_transfer
(transfer_type, transfer_model, shipping_transfer_rate)
select description[1]::varchar(2) as transfer_type,
       description[2]::text as transfer_model,
       shipping_transfer_rate::numeric(14,3)
from (select distinct shipping_transfer_rate, regexp_split_to_array(shipping_transfer_description, E'\\:+') as description
      from public.shipping) as shipping_information;

--shipping info
insert into public.shipping_info
(shippingid, shipping_country_id, agreementid, 
 transfer_type_id, shipping_plan_datetime,
 payment_amount, vendorid)
select shippingid, 
       scr.id as shipping_country_id, 
       sa.agreementid, 
       st.id as transfer_type_id,
       shipping_plan_datetime,
       payment_amount,
       vendorid
       from (select distinct shippingid,
                    shipping_country,
                    shipping_country_base_rate,
                    payment_amount,
                    vendorid,
                    (regexp_split_to_array(vendor_agreement_description, E'\\:+'))[1]::int as agreement_id,
                    (regexp_split_to_array(shipping_transfer_description, E'\\:+'))[1] as transfer_type,
                    (regexp_split_to_array(shipping_transfer_description, E'\\:+'))[2] as transfer_model,
                    shipping_plan_datetime
       from shipping s) as t1
       join shipping_agreement sa on t1.agreement_id = sa.agreementid
       join shipping_country_rates scr on t1.shipping_country = scr.shipping_country 
           and t1.shipping_country_base_rate = scr.shipping_country_base_rate
       join shipping_transfer st on t1.transfer_type = st.transfer_type and 
           t1.transfer_model = st.transfer_model
       order by shippingid;

      
--shipping_status
with last_status as (
   select shippingid,
          state, 
          status, 
          row_number () OVER (PARTITION BY shippingid 
                              ORDER BY state_datetime desc) as rn
   from public.shipping)
insert into public.shipping_status
(shippingid, state, status, shipping_start_fact_datetime, shipping_end_fact_datetime)
select lr.shippingid, 
       lr.state, 
       lr.status, 
       b.state_datetime as shipping_start_fact_datetime, 
       f.state_datetime as shipping_end_fact_datetime
   from last_status as lr
   left outer join (select shippingid, state_datetime
                    from public.shipping s
                    where state = 'booked') as b on lr.shippingid=b.shippingid
   left outer join (select shippingid, state_datetime
                    from public.shipping s
                    where state = 'recieved') as f on lr.shippingid=f.shippingid
   where rn = 1
   order by lr.shippingid;
