set mapreduce.map.memory.mb=12288;
set mapreduce.map.java.opts= -Xmx8192M -verbose:gc -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70;
set mapreduce.reduce.memory.mb=12288;
set mapreduce.reduce.java.opts= -Xmx8192M -verbose:gc -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70;



select union_table.transactions_aaapcc as transactions_aaapcc, sum(union_table.Basic_availability) as Basic_availability, 
sum(union_table.Basic_availability_with_airline_code) as Basic_availability_with_airline_code, sum(union_table.Direct_Access_airline_response) as Direct_Access_airline_response
from
(select final_table.sessionid as sessionid, final_table.transactions_aaapcc as transactions_aaapcc, sum(final_table.Basic_availability) as Basic_availability, sum(final_table.Basic_availability_with_airline_code) as Basic_availability_with_airline_code, 
count(final_table.transactions_messages_message) as Direct_Access_airline_response
from 
(select intable1.sessionid as sessionid, intable1.transactions_aaapcc as transactions_aaapcc, exploded_table.transactions_messages.message as transactions_messages_message, 
instr(exploded_table.transactions_messages.message, "$") as Basic_availability_with_airline_code, instr(exploded_table.transactions_messages.message, "$") as Basic_availability
from
(select hostsession.sessionid as sessionid, exploded_table.transactions_seq as transactions_seq, 
exploded_table.transactions_element.aaapcc as transactions_aaapcc, exploded_table.transactions_element.messages as transactions_messages 
from hostsession
lateral view posexplode(transactions)
exploded_table as transactions_seq, transactions_element
where year='2016' AND month in ("05","07") AND exploded_table.transactions_element.aaapcc IN 
("DP80",
"E37H",
"EP4C",
"ER3B",
"FJ67",
"FV8A",
"G3Z9",
"GK4A",
"H2MH",
"H4CC",
"H4P9",
"HV05",
"HV75",
"HW95",
"HX75",
"HZ1C",
"I7FG",
"IH2C",
"J1H3",
"J8W4",
"JM3H",
"JM5H",
"JN0H",
"K1MF",
"K433",
"KI49",
"LN4F",
"LZ4H",
"LZ7H",
"M0DC",
"M27B",
"M5PH",
"M8EG",
"MA0H",
"MB7H",
"MK6G",
"MO29",
"MQ8G",
"MR5G",
"MR7G",
"N0I2",
"N417",
"NB1C",
"NM19",
"Q381",
"QH99",
"QY39",
"RE0A",
"RF7C",
"RF9C",
"S19F",
"S20F",
"S28F",
"S5Z5",
"S64G",
"S71G",
"T4Q5",
"T5S5",
"T5T5",
"T6B5"
)
) intable1
lateral view posexplode(intable1.transactions_messages)
exploded_table as transactions_messages_seq, transactions_messages
where exploded_table.transactions_messages.message =  regexp_extract(exploded_table.transactions_messages.message, '1[0-9]?[0-9]?[A-Za-z]{3}?[A-Za-z]{6}\\@[A-Za-z]{2}\\.?\\.$', 0) 
AND exploded_table.transactions_messages.entrytype = "AVAIL" 
AND exploded_table.transactions_messages.messagetype = "RQ") final_table
GROUP BY
final_table.sessionid, final_table.transactions_aaapcc
union all
select final_table.sessionid as sessionid, final_table.transactions_aaapcc as transactions_aaapcc, sum(final_table.Basic_availability) as Basic_availability, 
count(final_table.transactions_messages_message) as Basic_availability_with_airline_code, sum(final_table.Direct_Access_airline_response) as Direct_Access_airline_response
from 
(select intable1.sessionid as sessionid, intable1.transactions_aaapcc as transactions_aaapcc, exploded_table.transactions_messages.message as transactions_messages_message, 
instr(exploded_table.transactions_messages.message, "@") as Basic_availability, instr(exploded_table.transactions_messages.message, "@") as Direct_Access_airline_response
from
(select hostsession.sessionid as sessionid, exploded_table.transactions_seq as transactions_seq, 
exploded_table.transactions_element.aaapcc as transactions_aaapcc, exploded_table.transactions_element.messages as transactions_messages 
from hostsession
lateral view posexplode(transactions)
exploded_table as transactions_seq, transactions_element
where year='2016' AND month in ("05","07") AND exploded_table.transactions_element.aaapcc IN 
("DP80",
"E37H",
"EP4C",
"ER3B",
"FJ67",
"FV8A",
"G3Z9",
"GK4A",
"H2MH",
"H4CC",
"H4P9",
"HV05",
"HV75",
"HW95",
"HX75",
"HZ1C",
"I7FG",
"IH2C",
"J1H3",
"J8W4",
"JM3H",
"JM5H",
"JN0H",
"K1MF",
"K433",
"KI49",
"LN4F",
"LZ4H",
"LZ7H",
"M0DC",
"M27B",
"M5PH",
"M8EG",
"MA0H",
"MB7H",
"MK6G",
"MO29",
"MQ8G",
"MR5G",
"MR7G",
"N0I2",
"N417",
"NB1C",
"NM19",
"Q381",
"QH99",
"QY39",
"RE0A",
"RF7C",
"RF9C",
"S19F",
"S20F",
"S28F",
"S5Z5",
"S64G",
"S71G",
"T4Q5",
"T5S5",
"T5T5",
"T6B5"
)
) intable1
lateral view posexplode(intable1.transactions_messages)
exploded_table as transactions_messages_seq, transactions_messages
where exploded_table.transactions_messages.message =  regexp_extract(exploded_table.transactions_messages.message, '1[0-9]?[0-9]?[A-Za-z]{3}?[A-Za-z]{6}\\$[A-Za-z]{2}\\.?\\.$', 0) 
AND exploded_table.transactions_messages.entrytype = "AVAIL" 
AND exploded_table.transactions_messages.messagetype = "RQ") final_table
GROUP BY
final_table.sessionid, final_table.transactions_aaapcc
union all
select final_table.sessionid as sessionid, final_table.transactions_aaapcc as transactions_aaapcc, count(final_table.transactions_messages_message) as Basic_availability, 
sum(final_table.Basic_availability_with_airline_code) as Basic_availability_with_airline_code, sum(final_table.Direct_Access_airline_response) as Direct_Access_airline_response
from 
(select intable1.sessionid as sessionid, intable1.transactions_aaapcc as transactions_aaapcc, exploded_table.transactions_messages.message as transactions_messages_message, 
instr(exploded_table.transactions_messages.message, "$") as Basic_availability_with_airline_code, instr(exploded_table.transactions_messages.message, "@") as Direct_Access_airline_response
from
(select hostsession.sessionid as sessionid, exploded_table.transactions_seq as transactions_seq, 
exploded_table.transactions_element.aaapcc as transactions_aaapcc, exploded_table.transactions_element.messages as transactions_messages 
from hostsession
lateral view posexplode(transactions)
exploded_table as transactions_seq, transactions_element
where year='2016' AND month in ("05","07") AND exploded_table.transactions_element.aaapcc IN 
("DP80",
"E37H",
"EP4C",
"ER3B",
"FJ67",
"FV8A",
"G3Z9",
"GK4A",
"H2MH",
"H4CC",
"H4P9",
"HV05",
"HV75",
"HW95",
"HX75",
"HZ1C",
"I7FG",
"IH2C",
"J1H3",
"J8W4",
"JM3H",
"JM5H",
"JN0H",
"K1MF",
"K433",
"KI49",
"LN4F",
"LZ4H",
"LZ7H",
"M0DC",
"M27B",
"M5PH",
"M8EG",
"MA0H",
"MB7H",
"MK6G",
"MO29",
"MQ8G",
"MR5G",
"MR7G",
"N0I2",
"N417",
"NB1C",
"NM19",
"Q381",
"QH99",
"QY39",
"RE0A",
"RF7C",
"RF9C",
"S19F",
"S20F",
"S28F",
"S5Z5",
"S64G",
"S71G",
"T4Q5",
"T5S5",
"T5T5",
"T6B5"
)
) intable1
lateral view posexplode(intable1.transactions_messages)
exploded_table as transactions_messages_seq, transactions_messages
where exploded_table.transactions_messages.message =  regexp_extract(exploded_table.transactions_messages.message, '1[0-9]?[0-9]?[A-Za-z]{3}?[A-Za-z]{6}\\.?\\.$', 0) 
AND exploded_table.transactions_messages.entrytype = "AVAIL" 
AND exploded_table.transactions_messages.messagetype = "RQ") final_table
GROUP BY
final_table.sessionid, final_table.transactions_aaapcc) union_table
group by 
union_table.transactions_aaapcc;