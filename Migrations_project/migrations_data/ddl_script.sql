--Creating new tables (target scheme)
drop table if exists public.shipping_info,
                     public.shipping_agreement,
                     public.shipping_country_rates,
                     public.shipping_transfer,
                     public.shipping_status;

--shipping_country_rates
create table public.shipping_country_rates (
	id serial NOT NULL,
	shipping_country text NULL,
	shipping_country_base_rate numeric(14,2) null,
	constraint shipping_country_rates_pkey primary key (id)
);

--shipping_agreement
create table public.shipping_agreement (
    agreementid int not null,
    agreement_number text,
    agreement_rate numeric(14,2),
    agreement_commission numeric(14,3),
    constraint shipping_agreement_pkey primary key (agreementid)
);

-- shipping_transfer
create table public.shipping_transfer (
id serial not null,
transfer_type varchar(2),
transfer_model text,
shipping_transfer_rate numeric(14,3),
constraint shipping_transfer_pkey primary key (id)
);

--shipping_info
create table public.shipping_info (
shippingid bigint not null,
shipping_country_id int,
agreementid int,
transfer_type_id int,
shipping_plan_datetime timestamp,
payment_amount numeric(14,2),
vendorid bigint,
constraint shipping_info_pkey primary key (shippingid),
constraint shipping_agreement_id_fkey foreign key (agreementid) references public.shipping_agreement(agreementid) ON UPDATE cascade,
constraint shipping_transfer_id_fkey foreign key (transfer_type_id) references public.shipping_transfer(id) ON UPDATE cascade,
constraint shipping_countru_id_fkey foreign key (shipping_country_id) references public.shipping_country_rates(id) on update cascade
);

--shipping_status
create table public.shipping_status (
shippingid bigint not null,
status text,
state text,
shipping_start_fact_datetime timestamp,
shipping_end_fact_datetime timestamp
);