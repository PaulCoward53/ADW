
BEGIN
 dbms_scheduler.create_schedule ('ADW_WORKDAY_00_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=000000;BYDAY=MON,TUE,WED,THU,FRI',comments =>'Workday (Mon-Fri) Midnight');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_01_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=010000;BYDAY=MON,TUE,WED,THU,FRI',comments =>'Workday (Mon-Fri) 1:00 AM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_02_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=020000;BYDAY=MON,TUE,WED,THU,FRI',comments =>'Workday (Mon-Fri) 2:00 AM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_03_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=030000;BYDAY=MON,TUE,WED,THU,FRI',comments =>'Workday (Mon-Fri) 3:00 AM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_04_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=040000;BYDAY=MON,TUE,WED,THU,FRI',comments =>'Workday (Mon-Fri) 4:00 AM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_05_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=050000;BYDAY=MON,TUE,WED,THU,FRI',comments =>'Workday (Mon-Fri) 5:00 AM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_06_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=060000;BYDAY=MON,TUE,WED,THU,FRI',comments =>'Workday (Mon-Fri) 6:00 AM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_07_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=070000;BYDAY=MON,TUE,WED,THU,FRI',comments =>'Workday (Mon-Fri) 7:00 AM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_08_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=080000;BYDAY=MON,TUE,WED,THU,FRI',comments =>'Workday (Mon-Fri) 8:00 AM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_09_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=090000;BYDAY=MON,TUE,WED,THU,FRI',comments =>'Workday (Mon-Fri) 9:00 AM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_10_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=100000;BYDAY=MON,TUE,WED,THU,FRI',comments =>'Workday (Mon-Fri) 10:00 AM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_11_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=110000;BYDAY=MON,TUE,WED,THU,FRI',comments =>'Workday (Mon-Fri) 11:00 AM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_12_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=120000;BYDAY=MON,TUE,WED,THU,FRI',comments =>'Workday (Mon-Fri) 12:00 AM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_13_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=130000;BYDAY=MON,TUE,WED,THU,FRI',comments =>'Workday (Mon-Fri) 1:00 PM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_14_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=140000;BYDAY=MON,TUE,WED,THU,FRI',comments =>'Workday (Mon-Fri) 2:00 PM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_15_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=150000;BYDAY=MON,TUE,WED,THU,FRI',comments =>'Workday (Mon-Fri) 3:00 PM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_16_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=160000;BYDAY=MON,TUE,WED,THU,FRI',comments =>'Workday (Mon-Fri) 4:00 PM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_17_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=170000;BYDAY=MON,TUE,WED,THU,FRI',comments =>'Workday (Mon-Fri) 5:00 PM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_18_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=180000;BYDAY=SUN,MON,TUE,WED,THU',comments =>'Workday (Sun-Thu) 6:00 PM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_19_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=190000;BYDAY=SUN,MON,TUE,WED,THU',comments =>'Workday (Sun-Thu) 7:00 PM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_20_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=200000;BYDAY=SUN,MON,TUE,WED,THU',comments =>'Workday (Sun-Thu) 8:00 PM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_21_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=210000;BYDAY=SUN,MON,TUE,WED,THU',comments =>'Workday (Sun-Thu) 9:00 PM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_22_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=220000;BYDAY=SUN,MON,TUE,WED,THU',comments =>'Workday (Sun-Thu) 10:00 PM');
 dbms_scheduler.create_schedule ('ADW_WORKDAY_23_00_SCH',repeat_interval =>'FREQ=DAILY;BYTIME=230000;BYDAY=SUN,MON,TUE,WED,THU',comments =>'Workday (Sun-Thu) 11:00 PM');

 -- START OF WEEKEND
 
 dbms_scheduler.create_schedule ('ADW_WEEKEND_01_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=180000;BYDAY=FRI',comments =>'Weekend (Fri) 06:00 PM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_02_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=190000;BYDAY=FRI',comments =>'Weekend (Fri) 07:00 PM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_03_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=200000;BYDAY=FRI',comments =>'Weekend (Fri) 08:00 PM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_04_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=210000;BYDAY=FRI',comments =>'Weekend (Fri) 09:00 PM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_05_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=220000;BYDAY=FRI',comments =>'Weekend (Fri) 10:00 PM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_06_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=230000;BYDAY=FRI',comments =>'Weekend (Fri) 11:00 PM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_07_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=000000;BYDAY=SAT',comments =>'Weekend (Sat) Midnight');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_08_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=010000;BYDAY=SAT',comments =>'Weekend (Sat) 01:00 AM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_09_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=020000;BYDAY=SAT',comments =>'Weekend (Sat) 02:00 AM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_10_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=030000;BYDAY=SAT',comments =>'Weekend (Sat) 03:00 AM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_11_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=040000;BYDAY=SAT',comments =>'Weekend (Sat) 04:00 AM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_12_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=050000;BYDAY=SAT',comments =>'Weekend (Sat) 05:00 AM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_13_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=050000;BYDAY=SAT',comments =>'Weekend (Sat) 06:00 AM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_14_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=070000;BYDAY=SAT',comments =>'Weekend (Sat) 07:00 AM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_15_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=080000;BYDAY=SAT',comments =>'Weekend (Sat) 08:00 AM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_16_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=090000;BYDAY=SAT',comments =>'Weekend (Sat) 09:00 AM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_17_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=100000;BYDAY=SAT',comments =>'Weekend (Sat) 10:00 AM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_18_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=110000;BYDAY=SAT',comments =>'Weekend (Sat) 11:00 AM');
 --- GAP FOR BACKUP OR ORACLE MAINTENANCE 
 dbms_scheduler.create_schedule ('ADW_WEEKEND_19_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=000000;BYDAY=SUN',comments =>'Weekend (Sun) Midnight');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_20_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=010000;BYDAY=SUN',comments =>'Weekend (Sun) 01:00 AM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_21_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=020000;BYDAY=SUN',comments =>'Weekend (Sun) 02:00 AM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_22_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=030000;BYDAY=SUN',comments =>'Weekend (Sun) 03:00 AM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_23_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=040000;BYDAY=SUN',comments =>'Weekend (Sun) 04:00 AM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_24_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=050000;BYDAY=SUN',comments =>'Weekend (Sun) 05:00 AM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_25_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=060000;BYDAY=SUN',comments =>'Weekend (Sun) 06:00 AM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_26_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=070000;BYDAY=SUN',comments =>'Weekend (Sun) 07:00 AM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_27_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=080000;BYDAY=SUN',comments =>'Weekend (Sun) 08:00 AM');
 dbms_scheduler.create_schedule ('ADW_WEEKEND_28_SCH',repeat_interval =>'FREQ=WEEKLY;BYTIME=090000;BYDAY=SUN',comments =>'Weekend (Sun) 09:00 AM');
 
 -- START OF MONTHLY
 dbms_scheduler.create_schedule ('ADW_MONTHLY_01_SCH',repeat_interval =>'FREQ=MONTHLY;BYTIME=100000;BYDAY=1SUN',comments =>'Monthy First(Sun) 10:00 AM');
 dbms_scheduler.create_schedule ('ADW_MONTHLY_02_SCH',repeat_interval =>'FREQ=MONTHLY;BYTIME=100000;BYDAY=2SUN',comments =>'Monthy Second (Sun) 10:00 AM');
 dbms_scheduler.create_schedule ('ADW_MONTHLY_03_SCH',repeat_interval =>'FREQ=MONTHLY;BYTIME=100000;BYDAY=3SUN',comments =>'Monthy Third(Sun) 10:00 AM');
 dbms_scheduler.create_schedule ('ADW_MONTHLY_04_SCH',repeat_interval =>'FREQ=MONTHLY;BYTIME=100000;BYDAY=4SUN',comments =>'Monthy Fourth(Sun) 10:00 AM');
END;
  