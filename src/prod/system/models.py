from datetime import datetime, date
from typing import Annotated

from sqlalchemy import (
    func,
    DateTime,
    Text,
    Boolean,
    Numeric,
    sql,
    UniqueConstraint,
    String
)

from sqlalchemy.orm import Mapped, DeclarativeBase, mapped_column, registry

str_3 = Annotated[str, 3]
str_64 = Annotated[str, 64]


class Base(DeclarativeBase):
    registry = registry(
        type_annotation_map={
            str_3: String(3),
            str_64: String(64),
        }
    )


# Кастомные типы данных
int_pk = Annotated[int, mapped_column(primary_key=True)]
created_at = Annotated[datetime, mapped_column(DateTime
                                               , server_default=func.now()
                                               , comment='Запись создана')
]
updated_at = Annotated[datetime, mapped_column(DateTime
                                               , server_default=func.now()
                                               , server_onupdate=func.now()  # Пиши триггер сам =(
                                               , comment='Запись обновлена')
]
is_active = Annotated[bool, mapped_column(Boolean
                                          , server_default=sql.true()
                                          , nullable=False
                                          , comment='Запись активна')
]
hash_address = Annotated[str_64, mapped_column(comment='хеш сумма адреса строки (кто и где)')]
hash_data = Annotated[str_64, mapped_column(comment='хеш сумма данных строки (что и когда)')]


# Декларативный стиль написания
class ParamRequests(Base):
    """ request and response statistics """
    __tablename__ = 'param_request'
    __table_args__ = {
        "schema": "comtrade",
        'comment': 'Хранение  запросов и ответов Comtrade'
    }

    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    hash_address: Mapped[hash_address | None]
    hash_data: Mapped[hash_data | None]

    parent: Mapped[int | None]  # = mapped_column(Integer, ForeignKey('param_requests.id'))
    request: Mapped[str | None] = mapped_column(Text, comment='Запрос')
    response: Mapped[str | None] = mapped_column(Text, comment='Ответ')
    status: Mapped[str | None] = mapped_column(comment='Ответа.Статус')
    size: Mapped[int | None] = mapped_column(comment='Ответа.Размер')


class ParamReturn(Base):
    """answers Comtrade"""
    __tablename__ = 'param_return'
    __table_args__ = (
        UniqueConstraint('hash_address'), {
            "schema": "comtrade",
            'comment': 'Параметры ответа'
        }
    )
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    hash_address: Mapped[hash_address | None]
    hash_data: Mapped[hash_data | None]

    param_requests_id: Mapped[int] = mapped_column(comment='ForeignKey("param_requests.id")')
    type_code: Mapped[str | None] = mapped_column(comment='Код типа: C для товаров и S для услуг')
    freq_code: Mapped[str | None] = mapped_column(comment='Код типа: A для годовых и M для ежемесячных')

    ref_year: Mapped[int | None] = mapped_column(comment='Год наблюдения')
    ref_month: Mapped[int | None] = mapped_column(
        comment='Месяц наблюдения. Для годового значения было бы установлено значение 52'
    )
    period: Mapped[str | None] = mapped_column(
        comment='Комбинация года и месяца (для месячных), год для (годовых)'
    )
    reporter_code: Mapped[int | None] = mapped_column(
        comment='Страна или географический район, к которым относится измеряемое статистическое явление')
    reporter_iso: Mapped[str | None] = mapped_column(comment='ISO 3 код репортера')

    flow_code: Mapped[str | None] = mapped_column(
        comment='Торговый поток или подпоток (экспорт, реэкспорт, импорт, повторный импорт и т.д.)'
    )
    #flow_desc: Mapped[str | None] = mapped_column(comment='Описание торговых потоков')
    partner_code: Mapped[str | None] = mapped_column(
        comment='Основная страна-партнер или географический район для соответствующего торгового потока'
    )
    partner_iso: Mapped[str_3 | None] = mapped_column(comment='ISO 3 код 1-го партнера')
    #partner_desc: Mapped[str | None] = mapped_column(comment='Описание 1-го партнера')
    partner2_code: Mapped[int | None] = mapped_column(
        comment='Второстепенная страна-партнер или географический район для соответствующего торгового потока'
    )
    partner2_iso: Mapped[str | None] = mapped_column(comment='Код ISO 3 2-го партнера')
    #partner2_desc: Mapped[str | None] = mapped_column(comment='Описание 2-го партнера')
    classification_code: Mapped[str | None] = mapped_column(
        comment='Указывает используемую классификацию продукта и версию (HS, SITC).'
    )
    classification_search_code: Mapped[str | None] = mapped_column(
        comment='Флажок, указывающий, представлена ли классификация по странам или нет'
    )
    is_original_classification: Mapped[bool | None] = mapped_column(comment='')
    cmd_code: Mapped[str | None] = mapped_column(comment='Код продукта в сочетании с классификационным кодом')
    cmd_desc: Mapped[str | None] = mapped_column(Text, comment='Описание категории товара/услуги')
    aggr_level: Mapped[int | None] = mapped_column(comment='Иерархический уровень категории товара/услуги')
    is_leaf: Mapped[bool | None] = mapped_column(
        comment='Определение того, имеет ли код товара самый базовый уровень (т.е. подзаголовок для ТН ВЭД)'
    )
    customs_code: Mapped[str | None] = mapped_column(comment='Таможенная или статистическая процедура')
    #customs_desc: Mapped[str | None] = mapped_column(comment='Описание таможенной процедуры')
    mos_code: Mapped[str | None] = mapped_column(
        comment='Способ поставки при оказании услуг (только торговля услугами)'
    )
    mot_code: Mapped[str | None] = mapped_column(
        comment='Вид транспорта, используемый, когда товары въезжают на экономическую территорию страны или покидают ее'
    )
    #mot_desc: Mapped[str | None] = mapped_column(comment='Описание вида транспорта')
    qty_unit_code: Mapped[int | None] = mapped_column(comment='Единица первичного количества')
    qty_unit_abbr: Mapped[str | None] = mapped_column(comment='Аббревиатура первичной единицы измерения количества')
    qty: Mapped[int | None] = mapped_column(Numeric, comment='Значение первичного количества')
    is_qty_estimated: Mapped[bool | None] = mapped_column(
        comment='Отметьте, подсчитано ли первичное количество или нет'
    )
    alt_qty_unit_code: Mapped[int | None] = mapped_column(comment='Единица вторичного количества')
    alt_qty_unit_abbr: Mapped[str | None] = mapped_column(comment='Аббревиатура вторичной единицы измерения количества')
    alt_qty: Mapped[int | None] = mapped_column(Numeric, comment='Значение вторичной величины')
    is_alt_qty_estimated: Mapped[bool | None] = mapped_column(
        comment='Отметьте, подсчитано ли вторичное количество или нет'
    )
    net_wgt: Mapped[int | None] = mapped_column(Numeric, comment='Вес нетто')
    is_net_wgt_estimated: Mapped[bool | None] = mapped_column(comment='Отметьте, указан ли расчетный вес нетто или нет')
    gross_wgt: Mapped[int | None] = mapped_column(Numeric, comment='Вес брутто')
    is_gross_wgt_estimated: Mapped[bool | None] = mapped_column(
        comment='Отметьте, указан ли расчетный вес брутто или нет'
    )
    cif_value: Mapped[int | None] = mapped_column(Numeric, comment='Стоимость импорта CIF')
    fob_value: Mapped[int | None] = mapped_column(Numeric, comment='Стоимость экспорта FOB')
    primary_value: Mapped[int | None] = mapped_column(
        Numeric, comment='Первичные торговые значения (взятые из значений CIF или FOB)'
    )
    legacy_estimation_flag: Mapped[str | None] = mapped_column(comment='Флаг оценки унаследованного количества')
    is_reported: Mapped[bool | None] = mapped_column(comment='Флажок, указывающий, представлена ли запись по стране')
    is_aggregate: Mapped[bool | None] = mapped_column(
        comment='Флаг, указывающий, агрегируется ли запись с помощью UNSD')


class DirectoryReporter(Base):
    __tablename__ = 'directory_reporter'
    __table_args__ = {
        "schema": "comtrade",
        'comment': 'Справочник DirectoryReporter'
    }
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    hash_address: Mapped[hash_address | None]
    hash_data: Mapped[hash_data | None]

    foreign_id: Mapped[int]
    text: Mapped[str]
    reporter_code: Mapped[str | None]
    reporter_desc: Mapped[str | None]
    reporter_note: Mapped[str | None]
    reporter_code_iso_alpha2: Mapped[str | None]
    reporter_code_iso_alpha3: Mapped[str | None]
    entry_effective_date: Mapped[datetime | None]
    is_group: Mapped[bool]
    entry_expired_date: Mapped[datetime | None]


class PartnerAreas(Base):
    __tablename__ = 'partner_areas'
    __table_args__ = {
        "schema": "comtrade",
        'comment': 'Справочник partnerAreas'
    }
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    hash_address: Mapped[hash_address | None]
    hash_data: Mapped[hash_data | None]

    foreign_id: Mapped[str]
    text: Mapped[str]
    partner_code: Mapped[str | None]
    partner_desc: Mapped[str | None]
    partner_note: Mapped[str | None]
    partner_code_iso_alpha2: Mapped[str | None]
    partner_code_iso_alpha3: Mapped[str | None]
    entry_effective_date: Mapped[datetime | None]
    is_group: Mapped[bool]


class Reporters(Base):
    __tablename__ = 'reporters'
    __table_args__ = {
        "schema": "comtrade",
        'comment': 'Справочник Reporters'
    }
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    hash_address: Mapped[hash_address | None]
    hash_data: Mapped[hash_data | None]

    foreign_id: Mapped[str]
    text: Mapped[str]
    reporter_code: Mapped[int | None]
    reporter_desc: Mapped[str | None]
    reporter_note: Mapped[str | None]
    reporter_code_iso_alpha2: Mapped[str | None]
    reporter_code_iso_alpha3: Mapped[str | None]
    entry_effective_date: Mapped[datetime | None]
    entry_expired_date: Mapped[datetime | None]
    is_group: Mapped[bool]


class CustomsCodes(Base):
    __tablename__ = 'customs_codes'
    __table_args__ = {
        "schema": "comtrade",
        'comment': 'Справочник CustomsCodes'
    }
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    hash_address: Mapped[hash_address | None]
    hash_data: Mapped[hash_data | None]

    foreign_id: Mapped[str]
    text: Mapped[str]


class Code(Base):
    """
    Список кодов гармонизированной системы Comtrade. Анализ показал, что он не полный.
    """
    __tablename__ = 'code'
    __table_args__ = {
        "schema": "comtrade",
        'comment': 'Справочник HS'
    }
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    hash_address: Mapped[hash_address | None]
    hash_data: Mapped[hash_data | None]

    foreign_id: Mapped[str | None] = mapped_column(comment='Код товарной категории')
    text: Mapped[str | None] = mapped_column(Text, comment='Описание товарной категории')
    parent: Mapped[str | None] = mapped_column(comment='Входит в группу')
    is_leaf: Mapped[int | None] = mapped_column(comment='')
    aggr_level: Mapped[int | None] = mapped_column(comment='уровень вложенности')
    standard_unit_abbr: Mapped[str | None] = mapped_column(comment='стандартные сокращения единиц измерения')
    hs: Mapped[str | None] = mapped_column(comment='Кодировка HS')
