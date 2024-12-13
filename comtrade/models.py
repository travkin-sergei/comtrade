"""
Модель данных прилождения.

Содержит комментарии к таблицам и столбцам в формате JSON.
Правила комментирования объектов в базе данных:
    +'https://confluence.exportcenter.ru/pages/viewpage.action?pageId=319785708'
"""

from sqlalchemy import (
    Text,
    Column,
    Integer,
    String,
    DateTime,
    Boolean,
    Numeric,
    UniqueConstraint,
    BigInteger,
    func,
    true,
)

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class ParamReturn(Base):
    """Параметры ответа."""

    __tablename__ = 'param_return'
    __table_args__ = (
        UniqueConstraint('hash_address'),
        {
            "schema": "comtrade",
            'comment': """{
                "name":"Параметры ответа",
                "npa":"https://comtradeapi.un.org/data/v1/get/{param['typeCode']}/{param['freqCode']}/HS?...",
        }"""
        }
    )
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_at = Column(
        DateTime,
        server_default=func.now(),
        comment='{"name":"Дата создания записи"}'
    )
    updated_at = Column(
        DateTime, server_default=func.now(),
        server_onupdate=func.now(),
        comment='{"name":"Дата обновления записи"}'
    )
    is_active = Column(
        Boolean,
        server_default=true(),
        nullable=False,
        comment='{"name":"Запись активна"}'
    )
    hash_address = Column(
        String(64),
        comment='{"name":"хеш сумма адреса строки","description":"dataset_checksum+period+reporter_code+flow_code+partner_code+cmd_code",}'
    )
    type_code = Column(String, comment='{"name":"Код типа: C для товаров и S для услуг"}')
    freq_code = Column(String, comment='{"name":"Код типа: A для годовых и M для ежемесячных"}')

    ref_year = Column(Integer, comment='{"name":"Год наблюдения"}')
    ref_month = Column(
        Integer,
        comment='{"name":"Месяц наблюдения. Для годового значения было бы установлено значение 52"}'
    )

    period = Column(String, comment='{"name":"года месяц (для месячных), год для (годовых)"}')
    reporter_code = Column(Integer, comment='{"name":"Страна или географический район"}')
    reporter_iso = Column(String, comment='{"name":"ISO 3 код репортера"}')
    flow_code = Column(
        String,
        comment='{"name":"Торговый поток или подпоток (экспорт, реэкспорт, импорт, повторный импорт и т.д.)"}'
    )
    flow_desc = Column(String, comment='{"name":"Описание торговых потоков"}')
    partner_code = Column(
        String,
        comment='{"name":"Основная страна-партнер или географический район для соответствующего торгового потока"}'
    )
    partner_iso = Column(String, comment='{"name":"ISO 3 код 1-го партнера"}')
    partner_desc = Column(String, comment='{"name":"Описание 1-го партнера"}')
    partner2_code = Column(
        Integer,
        comment='{"name":"Второстепенная страна-партнер или географический район для соответствующего торгового потока"}'
    )
    partner2_iso = Column(String, comment='{"name":"Код ISO 3 2-го партнера"}')
    partner2_desc = Column(String, comment='{"name":"Описание 2-го партнера"}')
    classification_code = Column(String, comment='{"name":"версия классификатора (HS, SITC)."}')
    classification_search_code = Column(
        String,
        comment='{"name":"Флажок, указывающий, представлена ли классификация по странам или нет"}'
    )
    is_original_classification = Column(Boolean)
    cmd_code = Column(String, comment='{"name":"Код продукта в сочетании с классификационным кодом"}')
    cmd_desc = Column(String, comment='{"name":"Описание категории товара/услуги"}')
    aggr_level = Column(
        Integer,
        comment='{"name":"Иерархический уровень категории товара/услуги", "description":"длинна поля cmd_code 2,4,6"}'
    )
    is_leaf = Column(
        Boolean,
        comment='{"name":"Определение того, имеет ли код товара самый базовый уровень (т.е. подзаголовок для ТН ВЭД)"}'
    )
    customs_code = Column(String, comment='{"name":"Таможенная или статистическая процедура"}')
    customs_desc = Column(String, comment='{"name":"Описание таможенной процедуры"}')
    mos_code = Column(
        String,
        comment='{"name":"Способ поставки при оказании услуг (только торговля услугами)"}'
    )
    mot_code = Column(
        String,
        comment='{"name":"Вид транспорта","description":"используемый, когда товары въезжают на экономическую территорию страны или покидают ее"}'
    )
    mot_desc = Column(String, comment='{"name":"Описание вида транспорта"}')
    qty_unit_code = Column(
        Integer,
        comment='{"name":"Единица первичного количества","description":"значение из справочника"}'
    )
    qty_unit_abbr = Column(String, comment='{"name":"Аббревиатура первичной единицы измерения количества"}')
    qty = Column(Numeric, comment='{"name":"Значение первичного количества"}')
    is_qty_estimated = Column(
        Boolean,
        comment='{"name":"Отметьте, подсчитано ли первичное количество или нет"}'
    )
    alt_qty_unit_code = Column(Integer, comment='{"name":"Единица вторичного количества"}')
    alt_qty_unit_abbr = Column(String, comment='{"name":"Аббревиатура вторичной единицы измерения количества"}')
    alt_qty = Column(Numeric, comment='{"name":"Значение вторичной величины"}')
    is_alt_qty_estimated = Column(Boolean, comment='{"name":"Отметьте, подсчитано ли вторичное количество или нет"}')
    net_wgt = Column(Numeric, comment='{"name":"Вес нетто"}')
    is_net_wgt_estimated = Column(Boolean, comment='{"name":"Отметьте, указан ли расчетный вес нетто или нет"}')
    gross_wgt = Column(Numeric, comment='{"name":"Вес брутто"}')
    is_gross_wgt_estimated = Column(Boolean, comment='{"name":"Отметьте, указан ли расчетный вес брутто или нет"}')
    cif_value = Column(Numeric, comment='{"name":"Стоимость импорта CIF"}')
    fob_value = Column(Numeric, comment='{"name":"Стоимость экспорта FOB"}')
    primary_value = Column(Numeric, comment='{"name":"Первичные торговые значения (взятые из значений CIF или FOB)"}')
    legacy_estimation_flag = Column(String, comment='{"name":"Флаг оценки унаследованного количества"}')
    is_reported = Column(Boolean, comment='{"name":"Флажок, указывающий, представлена ли запись по стране"}')
    is_aggregate = Column(Boolean, comment='{"name":"Флаг, указывающий, агрегируется ли запись с помощью UNSD"}')
    dataset_checksum = Column(
        BigInteger,
        comment='{"name":"хеш сумма данных", "description":"покаждой стране за отпределенный приод уникальная"}'
    )


class PartnerAreas(Base):
    __tablename__ = 'partner_areas'
    __table_args__ = {
        "schema": "comtrade",
        'comment': """{
            "name":"Справочник территорий (partnerAreas)",
            "npa":"https://comtradeapi.un.org/files/v1/app/reference/partnerAreas.json",
        }"""
    }

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_at = Column(
        DateTime,
        server_default=func.now(),
        comment='{"name":"Дата создания записи",}'
    )
    updated_at = Column(
        DateTime, server_default=func.now(),
        server_onupdate=func.now(),
        comment='{"name":"Дата обновления записи",}'
    )
    is_active = Column(
        Boolean,
        server_default=true(),
        nullable=False,
        comment='{"name":"Запись активна"}'
    )
    hash_address = Column(
        String,
        nullable=True,
        comment='{"name":"хеш сумма адреса строки","description":"foreign_id",}'
    )

    foreign_id = Column(Integer, nullable=False, comment='{"name":"код территории"}')
    text = Column(String, nullable=False)
    partner_code = Column(
        String, nullable=True,
        comment='{"name":"Основная страна-партнер или географический район для соответствующего торгового потока"}'
    )
    partner_desc = Column(String, nullable=True)
    partner_note = Column(String, nullable=True)
    partner_code_iso_alpha2 = Column(String, nullable=True)
    partner_code_iso_alpha3 = Column(String, nullable=True)
    entry_effective_date = Column(DateTime, nullable=True)
    is_group = Column(Boolean, nullable=False)


class HsCode(Base):
    """Список кодов гармонизированной системы Comtrade. Анализ показал, что он не полный."""

    __tablename__ = 'hs_code'
    __table_args__ = {
        "schema": "comtrade",
        'comment': """{
            "name":"Справочник гармонизированой системы (HS)",
            "description":"От H0 далее перебираем Н1,Н2, ...",
            "npa": "https://comtradeapi.un.org/files/v1/app/reference/H0.json",
        }
        """
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(
        DateTime,
        server_default=func.now(),
        comment='{"name":"Дата создания записи"}'
    )
    updated_at = Column(
        DateTime, server_default=func.now(),
        server_onupdate=func.now(),
        comment='{"name":"Дата обновления записи"}'
    )
    is_active = Column(
        Boolean,
        server_default=true(),
        nullable=False,
        comment='{"name":"Запись активна"}'
    )
    hash_address = Column(
        String,
        nullable=True,
        comment='{"name":"хеш сумма адреса строки","description":"cmd_code,hs",}'
    )

    hs = Column(String, nullable=True, comment='{"name":"Кодировка HS"}')
    cmd_code = Column(String, nullable=True, comment='{"name":"Код товарной категории"}')
    text = Column(Text, nullable=True, comment='{"name":"Описание товарной категории"}')
    parent = Column(String, nullable=True, comment='{"name":"Входит в группу"}')
    is_leaf = Column(Integer, nullable=True, )
    aggr_level = Column(Integer, nullable=True, comment='{"name":"уровень вложенности"}')
    standard_unit_abbr = Column(String, nullable=True, comment='{"name":"стандартные сокращения единиц измерения"}')


class VersionData(Base):
    """Версия данных."""

    __tablename__ = 'version_data'
    __table_args__ = {
        "schema": "comtrade",
        'comment': """{
            "name":"Версия данных",
            "npa":"https://comtradeapi.un.org/public/v1/getDA/C/A/HS",
        }
        """
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(
        DateTime,
        server_default=func.now(),
        comment='{"name":"Дата создания записи"}'
    )
    updated_at = Column(
        DateTime, server_default=func.now(),
        server_onupdate=func.now(),
        comment='{"name":"Дата обновления записи"}'
    )
    is_active = Column(
        Boolean,
        server_default=true(),
        nullable=False,
        comment='{"name":"Запись активна"}'
    )

    dataset_code = Column(BigInteger, nullable=False)
    type_code = Column(
        String, nullable=True,
        comment='{"name":"Код типа HS", "description":"C для товаров и S для услуг"}'
    )
    freq_code = Column(
        String, nullable=True,
        comment='{"name":"Код типа дат","description":"A для годовых и M для ежемесячных"}'
    )
    period = Column(
        String, nullable=True,
        comment='{"name":"период","description":"года месяц (для месячных), год для (годовых)"}'
    )
    reporter_code = Column(Integer, nullable=True, comment='{"name":"Страна или географический район"}')
    reporter_iso = Column(String, nullable=True, comment='{"name":"ISO 3 код репортера"}')
    reporter_desc = Column(String, nullable=True, comment='{"name":"наименование репортера"}')
    classification_code = Column(String, nullable=True, comment='{"name":"версия классификатора (HS, SITC)."}')
    classification_search_code = Column(
        String, nullable=True,
        comment='{"name":"представлена ли классификация по странам или нет"}'
    )
    is_original_classification = Column(Boolean, nullable=True, comment='{"name":"это оригинальная классификация"}')
    is_extended_flow_code = Column(Boolean, nullable=True)
    is_extended_partner_code = Column(Boolean, nullable=True)
    is_extended_partner2_code = Column(Boolean, nullable=True)
    is_extended_cmd_code = Column(Boolean, nullable=True)
    is_extended_customs_code = Column(Boolean, nullable=True)
    is_extended_mot_code = Column(Boolean, nullable=True)
    total_records = Column(Integer, nullable=True, comment='{"name":"количество записей"}')
    dataset_checksum = Column(BigInteger, nullable=True, comment='{"name":"хеш сумма данных"}')
    first_released = Column(DateTime, nullable=True, comment='{"name":"дата публикации"}')
    last_released = Column(DateTime, nullable=True, comment='{"name":"дата обновления"}')


class TradeRegimes(Base):
    """Хранение запросов с ошибкой."""

    __tablename__ = 'trade_regimes'
    __table_args__ = {
        "schema": "comtrade",
        'comment': """{
            "name":"торговые режимы",
            "description":"Код торгового потока или подток (экспорт, реэкспорт, импорт, повторный импорт и т.д.)",
            "npa":"https://comtradeapi.un.org/files/v1/app/reference/tradeRegimes.json",
        }
        """
    }
    id = Column(Integer, primary_key=True)
    created_at = Column(
        DateTime,
        server_default=func.now(),
        comment='{"name":"Дата создания записи"}'
    )
    updated_at = Column(
        DateTime, server_default=func.now(),
        server_onupdate=func.now(),
        comment='{"name":"Дата обновления записи"}'
    )
    is_active = Column(
        Boolean,
        server_default=true(),
        nullable=False,
        comment='{"name":"Запись активна"}'
    )
    hash_address = Column(
        String,
        nullable=True,
        comment='{"name":"хеш сумма адреса строки","description":"flow_code",}'
    )
    flow_code = Column(String, nullable=False, comment='{"name":"Код потока"}')
    flow_desc = Column(String, nullable=False, comment='{"name":"Расшифровка кода торгового потока"}')


class ErrorRequest(Base):
    """Хранение запросов с ошибкой."""

    __tablename__ = 'error_request'
    __table_args__ = {
        "schema": "comtrade",
        'comment': """{
            "name":"ошибки запросов",
            "description":"Здесь хранятся ошибки возникшие при запросах",
        }
        """
    }
    id = Column(Integer, primary_key=True)
    created_at = Column(
        DateTime,
        server_default=func.now(),
        comment='{"name":"Дата создания записи"}'
    )
    updated_at = Column(
        DateTime, server_default=func.now(),
        server_onupdate=func.now(),
        comment='{"name":"Дата обновления записи"}'
    )
    is_active = Column(
        Boolean,
        server_default=true(),
        nullable=False,
        comment='{"name":"Запись активна"}'
    )
    hash_address = Column(
        String,
        nullable=True,
        comment='{"name":"хеш сумма адреса строки","description":"dataset_checksum",}'
    )
    dataset_checksum = Column(
        BigInteger,
        comment='{"name":"хеш сумма данных", "description":"покаждой стране за отпределенный приод уникальная"}'
    )
    status_code = Column(Integer, nullable=False, comment='{"name":"Статус код get запроса"}')
    resp_code = Column(Integer, nullable=False, comment='{"name":"Код в теле ответа"}')
