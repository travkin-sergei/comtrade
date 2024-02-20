# Comtrade API

> Если не знаете как установить, то описание в конце статьи
# Назначение скрипта
Скачать данные по внешней торговле некоторых стран мира с сайта Comtrade
# Краткое описание 
Представляет из себя скрипт и базу данных.
Скрипт сначала формирует план запроса, а затем выполняет этот план.
План запроса записывается в базу данных. По мере исполнения плана происходит гашение записей и добавление данных ответа сервера.

## Цель:

1. Получить учетные данные
2. Скачать данные по внешней торговле
3. Проверить данные

## Задачи:

1. Получить учетные данные
    1. Зарегистрироваться на сайте
    2. Получить ключ доступа
2. Скачать данные
    1. Скачать справочники
    2. Сформировать план запроса
    3. Отправить запрос на
    4. Получить статистику запроса
    5. Получить данные
    6. Записать в базу данных результаты
3. Проверить данные
    1. Выявить дубликаты данных
    2. Выявить пропуски данных

## Технологии:

1. requests
2. Pandas
3. SQLAlchemy
4. Postgres

## Возможности:

+ Подключение к базе данных
+ Формирование плана запроса
+ Хранение параметров
    + Запрос - поле request в таблице param_requests
    + Ответ - поле response в таблице param_requests
+ Запись полученной информации

## Как работать со скриптом

+ Настроить базу данных (например, Postgres)
+ скачать справочники запустив файл __Main.py__
+ Сформировать план запросов __plan_request.py__
+ Запустить файл __parser.py__

## Файлы:

### main.py

Здесь настроено:

+ создание таблиц
+ обновление справочников из Comtrade

### site

#### list_error.py - список ошибок, которые удалось получить и обработать

#### plan_request.py - создание плана запроса для дальнейшей отправки

### system - системные файлы

#### config.py - конфигурация базы данных

+ Подключение к базе данных. Вам необходимо положить в корневую папку файл с названием test.env.
  или любым другим и поменять название в config.py.

### database.py - создание подключения к базе данных и описание некоторых типов данных в модели данных

Чтобы проверить, как выполняется запрос, необходимо заменить echo=True и смотри в терминал среды разработки,

### models.py - модель данных и SQL код для триггера и функции.

+ Объект ParamRequests или таблица param_requests - Хранение запросов в Comtrade
    + status = код статуса ответа на запрос
    + size - количество строк ответа
    + parent - ссылка на паспорт запроса или запрос до автоматического разбиения.
+ Объект ParamReturn или таблица param_return - Параметры ответа
    + sha256 - хеш сумма строки по полям в кодировке sha256. Помогает искать дубликаты. Требуется создать функцию и
      триггер.
      + расчеты хеш суммы в базе данных и в коде по алгоритму хеширования SHA-256 будут совпадать, если строки склеивать
```SQL
CREATE OR REPLACE FUNCTION public.param_return_update_hash_address()
RETURNS TRIGGER as
	$BODY$
	BEGIN
		NEW.hash_address = encode(sha256(CAST((
		                new.type_code    ||'+'|| new.freq_code     ||'+'||
		                new.period       ||'+'|| new.reporter_code ||'+'||
		                new.flow_code    ||'+'|| new.partner_code  ||'+'||
		                new.partner2_code||'+'|| new.cmd_code
	)AS text)::bytea), 'hex') ;
		return new;
	END;
	$BODY$ 
LANGUAGE plpgsql;
  
   
CREATE TRIGGER param_return_update_hash_address_TRIGGER
BEFORE INSERT or update on param_return
FOR EACH ROW
EXECUTE PROCEDURE param_return_update_hash_address();
```

Скорее всего потребуется построить индекс

```SQL
CREATE EXTENSION pg_trgm;
CREATE EXTENSION btree_gin;

CREATE INDEX param_return_hash_address_idx ON param_return USING gin (hash_address);

CREATE INDEX index_reporter_code ON param_return USING GIST (reporter_code)

/* Индес пригодится для скачивания по странам таблица comtrade_return столбец reporter_code*/
CREATE INDEX index_reporter_code_comtrade_return ON comtrade_return (reporter_code)
```

Удалить дубликаты
```SQL
with cte as
	(select *, 
		ROW_NUMBER() OVER(partition by hash_address order by id) as rn 
	from param_return)
delete from
param_return using cte
where param_return.id = cte.id and cte.rn > 1 
```

Поиск недостающих данных
```SQL
with 
 tnved2 as (select *,                                                                          cmd_code as tnved2 from param_return where aggr_level = 2 and period = '2022')
,tnved4 as (select distinct cmd_code, reporter_code, flow_desc, "period", left(param_return.cmd_code,2) as tnved2 from param_return where aggr_level = 4 and period = '2022') 
,tnved6 as (select distinct cmd_code, reporter_code, flow_desc, "period", left(param_return.cmd_code,4) as tnved4 from param_return where aggr_level = 6 and period = '2022') 
select 
	distinct
	 tnved2.cmd_code  as tnved_2
	,tnved4.cmd_code  as tnved_4
	,tnved6.cmd_code  as tnved_6
    ,tnved2."period"
	,tnved2.reporter_code
	,tnved2.partner_code
from tnved2
	full join tnved4 on tnved4.tnved2 = tnved2.tnved2   and tnved4.reporter_code = tnved2.reporter_code and tnved4.flow_desc = tnved2.flow_desc and tnved4."period" = tnved2."period" 
	full join tnved6 on tnved6.tnved4 = tnved4.cmd_code and tnved6.reporter_code = tnved4.reporter_code and tnved4.flow_desc = tnved2.flow_desc and tnved4."period" = tnved2."period" 
where (tnved2.cmd_code is null or tnved4.cmd_code is null or  tnved6.cmd_code is null)
	and tnved2.reporter_code != 8
```


+ Объект ComtradeReporter или таблица comtrade_reporter - Справочник географических объединений Comtrade
+ Объект ComtradePartner или comtrade_partner - Справочник Partner Comtrade
+ Объект ComtradeCmdH6 или comtrade_cmd_h6 - Справочник CMD:H6 из Comtrade

## key.txt

+ Файл должен находится в корневой директории и содержать ключи, которые необходимы для запроса.
+ Каждый бесплатный ключ имеет ограничение в 500 запросов или 411.82MB в сутки.

```
ХХХХХХХХХХХХХХХХХХХХХХХХХХХХХХХХ
ХХХХХХХХХХХХХХХХХХХХХХХХХХХХХХХХ
ХХХХХХХХХХХХХХХХХХХХХХХХХХХХХХХХ
ХХХХХХХХХХХХХХХХХХХХХХХХХХХХХХХХ
```

## test.env

- Файл должен содержать:

```
DB_HOST=db_user
DB_PORT=5432
DB_USER=db_user
DB_PASS=db_password
DB_NAME=db_name
```

## Ссылки:

[Адрес сайта](https://comtradeplus.un.org)

[Описание get запроса в Comtrade](https://comtradedeveloper.un.org/api-details#api=comtrade-v1&operation=get-get)

[Описание полей ответа с Comtrade](https://unstats.un.org/wiki/display/comtrade/New+Comtrade+FAQ+for+First+Time+Users#NewComtradeFAQforFirstTimeUsers-Canweswitchtodifferentlanguages?Whatarethelimitations)
> Скачать excel
> The __excel__ file contains description of data variables including their code lists.

[Ключ доступа будет здесь](https://comtradedeveloper.un.org/profile)

[Руководство пользователя](https://unstats.un.org/wiki/display/comtrade/New+Comtrade+User+Guide)

[Альтернативное решение от Comtrade](https://github.com/uncomtrade/comtradeapicall/blob/main/tests/example%20calling%20functions%20-%20script.py)

[Часто задаваемые вопросы](https://unstats.un.org/wiki/display/comtrade/New+Comtrade+FAQ+for+First+Time+Users)

[Справочники Comtrade](https://unstats.un.org/wiki/display/comtrade/UN+Comtrade+Reference+Tables)

## Как установить

1. Скачать проект на компьютер.
2. Создать файлы в папке где лежит ".gitignore"
    + key.txt
    + test.env
3. в командной строке вбить команду

```commandline
python -m pip install -r requirements.txt
```

Если выдаст

```commandline
A new release of pip is available: 23.2.1 -> 23.3.1
```

то

```commandline
python.exe -m pip install --upgrade pip
```

4. Открыть файл __main.py__ и запустить его.
5. Открыть файл __plan_request.py__
    + изучить комментарии и сформировать план запроса
    + запустить скрип
6. открыть и запустить файл __parser.py__ 
