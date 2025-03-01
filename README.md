# Problem
В некотором банке внедрили новую frontend-систему для работы с клиентами, а так же обновили и саму базу данных. Большую часть данных успешно были перенесены из старых БД в одну новую централизованную БД.  

Но в момент переключения со старой системы на новую возникли непредвиденные проблемы в ETL-процессе, небольшой период (конец 2017 начало 2018 года) так и остался в старой базе. Старую базу отключили, а не выгруженные данные сохранили в csv-файлы. 

Недавно банку потребовалось построить отчёт по 101 форме. Те данные что остались в csv-файлах тоже нужны. Загрузить их в новую БД не получиться из-за архитектурных и управленческих сложностей, нужно расчитать витрину отдельно. Но для этого сначала нужно загрузить исходные данные из csv-файлов в детальный слой (DS) хранилища в СУБД PostgreSQL.
# Task
- 1.1 Разработать ETL-процесс для загрузки банковских данных из csv-файлов в соответствующие таблицы СУБД PostgreSQL. Покрыть данный процесс логированием этапов работы и всевозможной дополнительной статистикой;
- 1.2 Расчитать витрины данных в слое DM: витрину оборотов и витрину остатков;
- 1.3 Необходимо произвести расчет 101 формы за январь 2018 года;
- 1.4 Написать скрипт, который позволит выгрузить данные из витрины dm. dm _f101_round_f в csv-файл;
# Showcase
- [1.1 python скрипт для загрузки и обработки данных в DS слой](https://disk.yandex.ru/d/pLCTm5upGyvuGA/project_work_1_1.mkv)(/etl_pipeline.py);
- [1.2 sql процедуры для расчета витрин оборотов и остатков в DM слое](https://disk.yandex.ru/d/pLCTm5upGyvuGA/project_work_1_2.mkv)(/sql_scripts/acc_turnover_balance.sql);
- [1.3 sql процедура для расчета 101 формы в DM слое](https://disk.yandex.ru/i/y94fISmyNbKOXQ)(/sql_scripts/f101_round.sql);
- [1.4 python скрипты по выгрузке и загрузке данных из 101 формы](https://disk.yandex.ru/i/37o3xbgL4_lkfA)(export_f101.py | import_f101.py);
# Technology
- PostgreSQL 13+;
- DBeaver, VSCode;
- Python 3.10;
- Ubuntu 19+;
- Docker;

