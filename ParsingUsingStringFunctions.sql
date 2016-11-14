set mapreduce.map.memory.mb=12288;
set mapreduce.map.java.opts= -Xmx8192M -verbose:gc -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70;
set mapreduce.reduce.memory.mb=12288;
set mapreduce.reduce.java.opts= -Xmx8192M -verbose:gc -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70;


select final_table.sessionid as sessionid, final_table.transactions_aaapcc as transactions_aaapcc, sum(final_table.basic_availability) as basic_availability, sum(final_table.basic_availability_with_airline_code) as basic_availability_with_airline_code
,sum(final_table.direct_access_airline_response) as direct_access_airline_response
from
(select intable1.sessionid as sessionid, intable1.transactions_aaapcc as transactions_aaapcc,
sum(instr(exploded_table.transactions_messages.message, "$")) AS Basic_availability,
sum(instr(exploded_table.transactions_messages.message, "$")) AS Basic_availability_with_airline_code,
count(substr(exploded_table.transactions_messages.message, 0, instr(exploded_table.transactions_messages.message, "@")-1)) AS Direct_Access_airline_response
from
(select hostsession.sessionid as sessionid, exploded_table.transactions_seq as transactions_seq, 
exploded_table.transactions_element.aaapcc as transactions_aaapcc, exploded_table.transactions_element.messages as transactions_messages 
from hostsession
lateral view posexplode(transactions)
exploded_table as transactions_seq, transactions_element
where year='2016' AND exploded_table.transactions_element.aaapcc IN 
( "DP80",
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
where 
exploded_table.transactions_messages.message like '1%' 
AND exploded_table.transactions_messages.entrytype = "AVAIL" 
AND exploded_table.transactions_messages.messagetype = "RQ"
AND instr(exploded_table.transactions_messages.message, ".") in (0, length(exploded_table.transactions_messages.message))
AND instr(exploded_table.transactions_messages.message, "$") = 0
AND instr(exploded_table.transactions_messages.message, "/") = 0
AND instr(exploded_table.transactions_messages.message, "*") = 0
AND instr(exploded_table.transactions_messages.message, "-") = 0
AND Locate("@", exploded_table.transactions_messages.message, (instr(exploded_table.transactions_messages.message, "@")+1)) = 0
AND substr(exploded_table.transactions_messages.message, instr(exploded_table.transactions_messages.message, "@"), length(exploded_table.transactions_messages.message)) like '@%'
AND LENGTH(substr(exploded_table.transactions_messages.message, 0, instr(exploded_table.transactions_messages.message, "@")-1)) IN (11, 12)
Group by
intable1.sessionid, intable1.transactions_aaapcc
union all
select intable1.sessionid as sessionid, intable1.transactions_aaapcc as transactions_aaapcc, 
count(substr(exploded_table.transactions_messages.message, 0, instr(exploded_table.transactions_messages.message, ".")-1)) AS Basic_availability,
sum(instr(exploded_table.transactions_messages.message, "$")) AS Basic_availability_with_airline_code,
sum(instr(exploded_table.transactions_messages.message, "@")) AS Direct_Access_airline_response
from
(select hostsession.sessionid as sessionid, exploded_table.transactions_seq as transactions_seq, 
exploded_table.transactions_element.aaapcc as transactions_aaapcc, exploded_table.transactions_element.messages as transactions_messages 
from hostsession
lateral view posexplode(transactions)
exploded_table as transactions_seq, transactions_element
where year='2016' AND exploded_table.transactions_element.aaapcc IN 
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
where exploded_table.transactions_messages.message like '1%' AND exploded_table.transactions_messages.entrytype = "AVAIL" AND exploded_table.transactions_messages.messagetype = "RQ"
AND substr(exploded_table.transactions_messages.message, instr(exploded_table.transactions_messages.message, "."), length(exploded_table.transactions_messages.message)) in ('', '.')
AND LENGTH(substr(exploded_table.transactions_messages.message, 0, instr(exploded_table.transactions_messages.message, ".")-1)) IN (11, 12)
AND instr(exploded_table.transactions_messages.message, "$") = 0
AND instr(exploded_table.transactions_messages.message, "@") = 0
Group by
intable1.sessionid, intable1.transactions_aaapcc
union all
select intable1.sessionid as sessionid, intable1.transactions_aaapcc as transactions_aaapcc, 
sum(instr(exploded_table.transactions_messages.message, "@")) AS Basic_availability,
count(substr(exploded_table.transactions_messages.message, 0, instr(exploded_table.transactions_messages.message, "$")-1)) AS Basic_availability_with_airline_code,
sum(instr(exploded_table.transactions_messages.message, "@")) AS Direct_Access_airline_response
from
(select hostsession.sessionid as sessionid, exploded_table.transactions_seq as transactions_seq, 
exploded_table.transactions_element.aaapcc as transactions_aaapcc, exploded_table.transactions_element.messages as transactions_messages 
from hostsession
lateral view posexplode(transactions)
exploded_table as transactions_seq, transactions_element
where year='2016' AND exploded_table.transactions_element.aaapcc IN 
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
where 
exploded_table.transactions_messages.message like '1%' 
AND exploded_table.transactions_messages.entrytype = "AVAIL" 
AND exploded_table.transactions_messages.messagetype = "RQ"
AND instr(exploded_table.transactions_messages.message, ".") in (0, length(exploded_table.transactions_messages.message))
AND instr(exploded_table.transactions_messages.message, "@") = 0
AND instr(exploded_table.transactions_messages.message, "/") = 0
AND instr(exploded_table.transactions_messages.message, "*") = 0
AND instr(exploded_table.transactions_messages.message, "-") = 0
AND Locate("$", exploded_table.transactions_messages.message, (instr(exploded_table.transactions_messages.message, "$")+1)) = 0
AND substr(exploded_table.transactions_messages.message, instr(exploded_table.transactions_messages.message, "$"), length(exploded_table.transactions_messages.message)) like '$%'
AND LENGTH(substr(exploded_table.transactions_messages.message, 0, instr(exploded_table.transactions_messages.message, "$")-1)) IN (11, 12)
Group by
intable1.sessionid, intable1.transactions_aaapcc) final_table
Group by
final_table.sessionid, final_table.transactions_aaapcc;