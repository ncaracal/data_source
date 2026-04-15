 1.4. need write rpoert TABLE to report/verify_with_binance_kline_api/{symbols}_startdate_enddate.report
    1.4.1 if len(symbols) < 2, show it, else show symbol count
    1.4.2 in reoprt need show check symbols
    1.4.3 if startdate_enddate in same day, just show single date



     4. need write rpoert TABLE to report/verify_self/{symbols}.report
    4.1 if len(symbols) <= 3, show it, else show symbol count
    4.2 in reoprt need show check symbols

report format, kline ex:

                   interval     exp    count  diff
-------------------------------------------------
parquet name             1m.    xxxx   xxxxx  xxx