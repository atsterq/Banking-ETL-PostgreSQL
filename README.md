# Description
В некотором банке внедрили новую frontend-систему для работы с клиентами, а так же обновили и саму базу данных. Большую часть данных успешно были перенесены из старых БД в одну новую централизованную БД.  

Но в момент переключения со старой системы на новую возникли непредвиденные проблемы в ETL-процессе, небольшой период (конец 2017 начало 2018 года) так и остался в старой базе. Старую базу отключили, а не выгруженные данные сохранили в csv-файлы. 

Недавно банку потребовалось построить отчёт по 101 форме. Те данные что остались в csv-файлах тоже нужны. Загрузить их в новую БД не получиться из-за архитектурных и управленческих сложностей, нужно рассчитать витрину отдельно. Но для этого сначала нужно загрузить исходные данные из csv-файлов в детальный слой (DS) хранилища в СУБД PostgreSQL.
# Showcase
- [1.1 etl_pipeline](https://disk.yandex.ru/d/pLCTm5upGyvuGA/project_work_1_1.mkv)(/etl_pipeline.py)
- [1.2 sql procedures for calculating the turnover and balance marts](https://disk.yandex.ru/d/pLCTm5upGyvuGA/project_work_1_2.mkv)(/sql_scripts/acc_turnover_balance.sql)
- [1.3 sql procedure for calculating 101 form]()(/sql_scripts/f101_round.sql)
# Technology
- PostgreSQL 13+;
- DBeaver, VSCode;
- Python 3.10;
- Ubuntu 19+;
- Docker;

