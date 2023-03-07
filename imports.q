
// write function to write data into partition 

schema: "JFFFFFJFIFFIDS"
csv_data_path: "D:/crypto/data/dates/"
database_path: ":D:/crypto/data/db2"
database_path: ":/Users/salom/workspace/crypto/data/db2"


load_kline_for_date: {(schema;enlist",") 0: `$csv_data_path , ssr[string[x]; "."; ""],".csv"}

python_to_kdb_datetime: {"p" $ 1000000 * (x - 10957 * 3600 * 24 * 1000)}

cast_kline: {update sym: `p#sym, open_time: python_to_kdb_datetime open_time, close_time: python_to_kdb_datetime close_time from x}

save_kline_partition_date : {(`$database_path,"/",string[x],"/kline" ) set .Q.en[`$database_path; delete date from cast_kline load_kline_for_date x]}


cast_kline load_kline_for_date 2022.01.05

start_date: 2022.01.01
date_list: start_date + .z.D - start_date
check: save_kline_partition_date each date_list


