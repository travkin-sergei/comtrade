# Обновление данных Comtrade

## Запуск приложения
Обновление pip
```bash
  python.exe -m pip install --upgrade pip
```

Устаноить зависимости
```bash
  pip install -r requirements.txt
```

В корневой дириктори создать файл ".env" (Скопируй и вставь в терминал.)
+ Eсли количество ключей больше 1, то указать ключи через запятую

```bash
@"
DEBUG=True
COMTRADE_KEY=********************
DATABASE_URL=postgres://postgres:postgres@localhost:5432/world_statistics
URL_BASE=**************
URL_MONTH=**************
URL_YEAR=**************
"@ | Set-Content -Path .env
```

Непосредственный запуск приложения
```bash
  python -m comtrade.main
```


## Технологии:

1. requests
3. SQLAlchemy

## Пояснения

+ Подключение к базе данных
+ Запись полученной информации
+ Каждый бесплатный ключ имеет ограничение в 500 запросов или 411.82MB в сутки.

## Проверка результатов
Количество строк из запроса дынных должно соответствовать  
количеству строк из данных из API https://comtradeapi.un.org/public/v1/getDA/C/A/HS  
При условии, что при запросе данных были запрошены все варианты торговых режимов (TradeRegimes)  

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
