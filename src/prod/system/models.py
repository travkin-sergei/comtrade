from datetime import datetime, date
from typing import Annotated
from sqlalchemy import (
    func,
    DateTime,
    Text,
    Boolean,
    Numeric,
    sql,
    ForeignKey, Integer, UniqueConstraint
)

from sqlalchemy.orm import Mapped, mapped_column
from src.prod.system.database import Base, str_3, str_64

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
    __tablename__ = 'param_request'
    __table_args__ = {
        'comment': 'Хранение  запросов в Comtrade'
    }

    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    parent: Mapped[int | None]  # = mapped_column( ForeignKey('param_requests.id'))
    request: Mapped[str | None] = mapped_column(Text, comment='Запрос')
    response: Mapped[str | None] = mapped_column(Text, comment='Ответ')
    status: Mapped[str | None] = mapped_column(comment='Ответа.Статус')
    size: Mapped[int | None] = mapped_column(comment='Ответа.Размер')


class ParamReturn(Base):
    __tablename__ = 'param_return'
    __table_args__ = (
        UniqueConstraint('sha256'), {
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
    ref_period_id: Mapped[str | None] = mapped_column(
        comment='Период времени, к которому относится измеренное наблюдение')
    ref_year: Mapped[str | None] = mapped_column(comment='Год наблюдения')
    ref_month: Mapped[str | None] = mapped_column(
        comment='Месяц наблюдения. Для годового значения было бы установлено значение 52')
    period: Mapped[str | None] = mapped_column(
        comment='Комбинация года и месяца (для месячных), год для (годовых)')
    reporter_code: Mapped[int | None] = mapped_column(
        comment='Страна или географический район, к которым относится измеряемое статистическое явление')
    reporter_iso: Mapped[str | None] = mapped_column(comment='ISO 3 код репортера')
    reporter_desc: Mapped[str | None] = mapped_column(comment='Описание репортера')
    flow_code: Mapped[str | None] = mapped_column(
        comment='Торговый поток или подпоток (экспорт, реэкспорт, импорт, повторный импорт и т.д.)')
    flow_desc: Mapped[str | None] = mapped_column(comment='Описание торговых потоков')
    partner_code: Mapped[int | None] = mapped_column(
        comment='Основная страна-партнер или географический район для соответствующего торгового потока')
    partner_iso: Mapped[str_3 | None] = mapped_column(comment='ISO 3 код 1-го партнера')
    partner_desc: Mapped[str | None] = mapped_column(comment='Описание 1-го партнера')
    partner2_code: Mapped[int | None] = mapped_column(
        comment='Второстепенная страна-партнер или географический район для соответствующего торгового потока')
    partner2_iso: Mapped[str | None] = mapped_column(comment='Код ISO 3 2-го партнера')
    partner2_desc: Mapped[str | None] = mapped_column(comment='Описание 2-го партнера')
    classification_code: Mapped[str | None] = mapped_column(
        comment='Указывает используемую классификацию продукта и версию (HS, SITC).')
    classification_search_code: Mapped[str | None] = mapped_column(
        comment='Флажок, указывающий, представлена ли классификация по странам или нет')
    is_original_classification: Mapped[bool | None] = mapped_column(comment='')
    cmd_code: Mapped[str | None] = mapped_column(comment='Код продукта в сочетании с классификационным кодом')
    cmd_desc: Mapped[str | None] = mapped_column(Text, comment='Описание категории товара/услуги')
    aggr_level: Mapped[int | None] = mapped_column(comment='Иерархический уровень категории товара/услуги')
    is_leaf: Mapped[bool | None] = mapped_column(
        comment='Определение того, имеет ли код товара самый базовый уровень (т.е. подзаголовок для ТН ВЭД)')
    customs_code: Mapped[str | None] = mapped_column(comment='Таможенная или статистическая процедура')
    customs_desc: Mapped[str | None] = mapped_column(comment='Описание таможенной процедуры')
    mos_code: Mapped[str | None] = mapped_column(
        comment='Способ поставки при оказании услуг (только торговля услугами)')
    mot_code: Mapped[str | None] = mapped_column(
        comment='Вид транспорта, используемый, когда товары въезжают на экономическую территорию страны или покидают ее')
    mot_desc: Mapped[str | None] = mapped_column(comment='Описание вида транспорта')
    qty_unit_code: Mapped[int | None] = mapped_column(comment='Единица первичного количества')
    qty_unit_abbr: Mapped[str | None] = mapped_column(comment='Аббревиатура первичной единицы измерения количества')
    qty: Mapped[int | None] = mapped_column(Numeric, comment='Значение первичного количества')
    is_qty_estimated: Mapped[bool | None] = mapped_column(
        comment='Отметьте, подсчитано ли первичное количество или нет')
    alt_qty_unit_code: Mapped[int | None] = mapped_column(comment='Единица вторичного количества')
    alt_qty_unit_abbr: Mapped[str | None] = mapped_column(comment='Аббревиатура вторичной единицы измерения количества')
    alt_qty: Mapped[int | None] = mapped_column(Numeric, comment='Значение вторичной величины')
    is_alt_qty_estimated: Mapped[bool | None] = mapped_column(
        comment='Отметьте, подсчитано ли вторичное количество или нет')
    net_wgt: Mapped[int | None] = mapped_column(Numeric, comment='Вес нетто')
    is_net_wgt_estimated: Mapped[bool | None] = mapped_column(comment='Отметьте, указан ли расчетный вес нетто или нет')
    gross_wgt: Mapped[int | None] = mapped_column(Numeric, comment='Вес брутто')
    is_gross_wgt_estimated: Mapped[bool | None] = mapped_column(
        comment='Отметьте, указан ли расчетный вес брутто или нет')
    cif_value: Mapped[int | None] = mapped_column(Numeric, comment='Торговые ценности в CIF')
    fob_value: Mapped[int | None] = mapped_column(Numeric, comment='Торговые ценности на условиях FOB')
    primary_value: Mapped[int | None] = mapped_column(Numeric,
                                                      comment='Первичные торговые значения (взятые из значений CIF или FOB)')
    legacy_estimation_flag: Mapped[str | None] = mapped_column(comment='Флаг оценки унаследованного количества')
    is_reported: Mapped[bool | None] = mapped_column(comment='Флажок, указывающий, представлена ли запись по стране')
    is_aggregate: Mapped[bool | None] = mapped_column(
        comment='Флаг, указывающий, агрегируется ли запись с помощью UNSD')


class ComtradeReporter(Base):
    __tablename__ = 'comtrade_reporter'
    __table_args__ = {
        'comment': 'Справочник географических объединений Comtrade'
    }
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    foreign_id: Mapped[int]
    text: Mapped[str]
    reporter_code: Mapped[int | None]
    reporter_desc: Mapped[str | None]
    reporter_note: Mapped[str | None]
    reporter_code_iso_alpha2: Mapped[str | None]
    reporter_code_iso_alpha3: Mapped[str | None]
    entry_effective_date: Mapped[datetime | None]
    is_group: Mapped[bool]
    entry_expired_date: Mapped[datetime | None]


class ComtradePartner(Base):
    __tablename__ = 'comtrade_partner'
    __table_args__ = {
        'comment': 'Справочник Partner Comtrade'
    }
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    foreign_id: Mapped[int]
    text: Mapped[str]
    partner_code: Mapped[int | None]
    partner_desc: Mapped[str | None]
    partner_note: Mapped[str | None]
    partner_code_iso_alpha2: Mapped[str | None]
    partner_code_iso_alpha3: Mapped[str | None]
    entry_effective_date: Mapped[datetime | None]
    entry_expired_date: Mapped[datetime | None]
    is_group: Mapped[bool]


class ComtradeCmdH6(Base):
    __tablename__ = 'comtrade_cmd_h6'
    __table_args__ = {
        'comment': 'Справочник CMD:H6 из Comtrade'
    }
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    foreign_id: Mapped[str | None] = mapped_column(comment='Код товарной категории')
    text: Mapped[str | None] = mapped_column(Text, comment='Описание товарной категории')
    parent: Mapped[str | None] = mapped_column(comment='Входит в группу')


class TnVed(Base):
    __tablename__ = 'tn_ved'
    __table_args__ = {
        'comment': 'Справочник ТНВЭД'
    }
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    date_start: Mapped[date] = mapped_column(comment='Дата начала действия')
    date_stop: Mapped[date | None] = mapped_column(comment='Дата окончания действия')
    len_code: Mapped[int | None] = mapped_column(comment='Длинна строки колонки code')
    code: Mapped[str | None] = mapped_column(comment='Код товарной категории')


class Language(Base):
    __tablename__ = 'language'
    __table_args__ = {
        'comment': 'Языки мира'
    }
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    lang_name: Mapped[str | None] = mapped_column(comment='Название языка')


class Territory(Base):
    __tablename__ = 'territory'
    __table_args__ = {
        'comment': 'Территория. Идентификаторы'
    }
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    name: Mapped[str | None] = mapped_column(
        comment='Территория. Названия (не регламентировано! не применять в работе)')


class TerritoryCode(Base):
    __tablename__ = 'territory_code'
    __table_args__ = {
        'comment': 'Территория. Коды'
    }
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    date_start: Mapped[date | None]
    date_stop: Mapped[date | None]
    territory_id: Mapped[int] = mapped_column(Integer, ForeignKey('territory.id'))
    code: Mapped[str]
    territory_code_type_id: Mapped[int] = mapped_column(Integer, ForeignKey('territory_code_type.id'))


class TerritoryCodeType(Base):
    __tablename__ = 'territory_code_type'
    __table_args__ = {
        'comment': 'Территория. Коды. Типы'
    }
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    code_type: Mapped[str]


class TerritoryMainSub(Base):
    __tablename__ = 'territory_main_sub'
    __table_args__ = {
        'comment': 'Территория. Связи'
    }
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    date_start: Mapped[date | None]
    date_stop: Mapped[date | None]
    territory_main: Mapped[int] = mapped_column(Integer, ForeignKey('territory.id'))
    territory_sub: Mapped[int] = mapped_column(Integer, ForeignKey('territory.id'))


class TerritoryMainSubName(Base):
    __tablename__ = 'territory_main_sub_name'
    __table_args__ = {
        'comment': 'Территория. Связи. Названия'
    }
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    territory_main_sub_id: Mapped[int] = mapped_column(Integer, ForeignKey('territory_main_sub.id'))
    territory_name_id: Mapped[int] = mapped_column(Integer, ForeignKey('territory_name.id'))
    ranging: Mapped[int]
    variant: Mapped[int]


class TerritoryName(Base):
    __tablename__ = 'territory_name'
    __table_args__ = {
        'comment': 'Территория. Названия.'
    }
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    territory_id: Mapped[int] = mapped_column(Integer, ForeignKey('territory.id'))
    language_id: Mapped[int] = mapped_column(Integer, ForeignKey('language.id'))
    name: Mapped[str]
    description: Mapped[str]


class TerritoryGeo(Base):
    __tablename__ = 'territory_geo'
    __table_args__ = {
        'comment': 'Территория. Координаты.'
    }
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    territory_id: Mapped[int] = mapped_column(Integer, ForeignKey('territory.id'))
    geo_json: Mapped[str] = mapped_column(Text, comment='Территория. Географические координаты')
