with user_group_messages as (
    select hg.hk_group_id,
    COUNT(distinct hu.hk_user_id) as cnt_users_in_group_with_messages
    from DWH.h_dialogs hd
    inner join DWH.l_user_message lum on hd.hk_message_id = lum.hk_message_id 
    inner join DWH.h_users hu on lum.hk_user_id = hu.hk_user_id 
    inner join DWH.l_groups_dialogs lgd on hd.hk_message_id = lgd.hk_message_id 
    inner join DWH.h_groups hg on lgd.hk_group_id = hg.hk_group_id
    group by hg.hk_group_id
),
    user_group_log as (
    select luga.hk_group_id ,
           count(DISTINCT luga.hk_user_id) as cnt_added_users
    from DWH.s_auth_history sah 
    left join DWH.l_user_group_activity luga on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity 
    where sah.hk_l_user_group_activity in (
    									select hk_l_user_group_activity 
    									from DWH.l_user_group_activity 
   										where hk_group_id in (
                        									 select hk_group_id 
                   									         from DWH.h_groups
                        									 where hk_group_id in (select hk_group_id from user_group_messages)
                         									 order by registration_dt
                         									 limit 10
                          									)
                          				) 
           and sah.event = 'add'
    group by luga.hk_group_id
    order by cnt_added_users
)
select  ugl.hk_group_id, 
        ugl.cnt_added_users,
        ugm.cnt_users_in_group_with_messages,
        ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users as group_conversion       
from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc;


-- check 1
--    select hk_group_id,
--           cnt_users_in_group_with_messages
--    from user_group_messages
--    order by cnt_users_in_group_with_messages
--    limit 10;
--|hk_group_id              |cnt_users_in_group_with_messages|
--|-------------------------|--------------------------------|
--|3,573,821,581,643,801,282|3                               |
--|6,210,160,447,255,813,510|30                              |
--|8,764,442,430,478,790,635|39                              |
--|1,319,692,034,796,300,830|55                              |
--|4,038,157,487,054,977,717|66                              |
--|6,640,162,946,765,271,748|69                              |
--|1,594,722,103,625,592,852|75                              |
--|5,504,994,290,564,188,728|102                             |
--|7,271,205,554,897,122,496|109                             |
--|4,651,097,343,657,001,468|112                             |


--check 2
--select hk_group_id
--            ,cnt_added_users
--from user_group_log
--order by cnt_added_users
--limit 10
--;
--|hk_group_id              |cnt_added_users|
--|-------------------------|---------------|
--|7,279,971,728,630,971,062|1,914          |
--|7,757,992,142,189,260,835|2,505          |
--|6,014,017,525,933,240,454|3,405          |
--|2,461,736,748,292,367,987|3,575          |
--|9,183,043,445,192,227,260|3,725          |
--|3,214,410,852,649,090,659|3,781          |
--|4,350,425,024,258,480,878|4,172          |
--|5,568,963,519,328,366,880|4,298          |
--|206,904,954,090,724,337  |4,311          |
--|7,174,329,635,764,732,197|4,794          |


--answer to business
--|hk_group_id              |cnt_added_users|cnt_users_in_group_with_messages|group_conversion|
--|-------------------------|---------------|--------------------------------|----------------|
--|206,904,954,090,724,337  |4,311          |1,925                           |0.4465321271    |
--|7,174,329,635,764,732,197|4,794          |2,140                           |0.4463913225    |
--|4,350,425,024,258,480,878|4,172          |1,831                           |0.4388782359    |
--|9,183,043,445,192,227,260|3,725          |1,605                           |0.4308724832    |
--|2,461,736,748,292,367,987|3,575          |1,539                           |0.4304895105    |
--|3,214,410,852,649,090,659|3,781          |1,612                           |0.4263422375    |
--|5,568,963,519,328,366,880|4,298          |1,827                           |0.4250814332    |
--|6,014,017,525,933,240,454|3,405          |1,361                           |0.3997063142    |
--|7,757,992,142,189,260,835|2,505          |880                             |0.3512974052    |
--|7,279,971,728,630,971,062|1,914          |646                             |0.3375130617    |



