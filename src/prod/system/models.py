from datetime import datetime
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


# Декларативный стиль написания
class ParamRequests(Base):
    __tablename__ = 'param_request'
    __table_args__ = {
        'schema': 'public',
        'comment': 'Хранение  запросов в Comtrade'
    }

    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    parent: Mapped[int | None]  # = mapped_column(Integer,ForeignKey('param_requests.id'))
    request: Mapped[str | None] = mapped_column(Text, comment='Запрос')
    response: Mapped[str | None] = mapped_column(Text, comment='Ответ')
    status: Mapped[str | None] = mapped_column(comment='Ответа.Статус')
    size: Mapped[int | None] = mapped_column(comment='Ответа.Размер')


class ParamReturn(Base):
    __tablename__ = 'param_return'
    __table_args__ = (
        UniqueConstraint('sha256'), {
            'schema': 'public',
            'comment': 'Параметры ответа'

        }
    )
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    sha256: Mapped[str_64 | None] = mapped_column(
        comment='hash_sum=sha256(type_code,freq_code,period,reporter_code,flow_code,partner_code,partner2_code,cmd_code'
    )
    param_requests_id: Mapped[int] = mapped_column(comment='ForeignKey("param_requests.id")')
    type_code: Mapped[str | None] = mapped_column(comment='Тип торговли: C для товаров и S для услуг')
    freq_code: Mapped[str | None] = mapped_column(comment='Частота торговли: A для годовых и M для ежемесячных')
    ref_period_id: Mapped[str | None] = mapped_column(comment='')
    ref_year: Mapped[str | None] = mapped_column(comment='')
    ref_month: Mapped[str | None] = mapped_column(comment='')
    period: Mapped[str | None] = mapped_column(
        comment='Год= ГГГГ Месяц = ГГГГММ.')
    reporter_code: Mapped[int | None] = mapped_column(
        comment='Код репортера, ForeignKey("comtrade_reporter.foreign_id")')
    reporter_iso: Mapped[str | None] = mapped_column(comment='')
    reporter_desc: Mapped[str | None] = mapped_column(comment='')
    flow_code: Mapped[str | None] = mapped_column(comment='Код направления перемещения X-Экспорт M - Импорт')
    flow_desc: Mapped[str | None] = mapped_column(comment='')
    partner_code: Mapped[int | None] = mapped_column(
        comment='Код партнера ForeignKey("comtrade_partner.foreign_id")')
    partner_iso: Mapped[str_3 | None] = mapped_column(comment='ISO код страны')
    partner_desc: Mapped[str | None] = mapped_column(comment='')
    partner2_code: Mapped[int | None] = mapped_column(comment='Код отправителя (возможные значения - код M49 стран)')
    partner2_iso: Mapped[str | None] = mapped_column(comment='')
    partner2_desc: Mapped[str | None] = mapped_column(comment='')
    classification_code: Mapped[str | None] = mapped_column(comment='')
    classification_search_code: Mapped[str | None] = mapped_column(comment='')
    is_original_classification: Mapped[bool | None] = mapped_column(comment='')
    cmd_code: Mapped[str | None] = mapped_column(
        comment='Код товара, ForeignKey("comtrade_cmd_h6.foreign_id")')
    cmd_desc: Mapped[str | None] = mapped_column(Text, comment='')
    aggr_level: Mapped[int | None] = mapped_column(comment='Уровень агрегации')
    is_leaf: Mapped[bool | None] = mapped_column(comment='')
    customs_code: Mapped[str | None] = mapped_column(comment='Таможенный код')
    customs_desc: Mapped[str | None] = mapped_column(comment='')
    mos_code: Mapped[str | None] = mapped_column(comment='')
    mot_code: Mapped[str | None] = mapped_column(comment='Код вида транспорта')
    mot_desc: Mapped[str | None] = mapped_column(comment='')
    qty_unit_code: Mapped[int | None] = mapped_column(comment='')
    qty_unit_abbr: Mapped[str | None] = mapped_column(comment='')
    qty: Mapped[int | None] = mapped_column(Numeric, comment='')
    is_qty_estimated: Mapped[bool | None] = mapped_column(comment='')
    alt_qty_unit_code: Mapped[int | None] = mapped_column(comment='')
    alt_qty_unit_abbr: Mapped[str | None] = mapped_column(comment='')
    alt_qty: Mapped[int | None] = mapped_column(Numeric, comment='')
    is_alt_qty_estimated: Mapped[bool | None] = mapped_column(comment='')
    net_wgt: Mapped[int | None] = mapped_column(Numeric, comment='')
    is_net_wgt_estimated: Mapped[bool | None] = mapped_column(comment='')
    gross_wgt: Mapped[int | None] = mapped_column(Numeric, comment='')
    is_gross_wgt_estimated: Mapped[bool | None] = mapped_column(comment='')
    cif_value: Mapped[int | None] = mapped_column(Numeric, comment='')
    fob_value: Mapped[int | None] = mapped_column(Numeric, comment='')
    primary_value: Mapped[int | None] = mapped_column(Numeric, comment='')
    legacy_estimation_flag: Mapped[str | None] = mapped_column(comment='')
    is_reported: Mapped[bool | None] = mapped_column(comment='')
    is_aggregate: Mapped[bool | None] = mapped_column(comment='')


class ComtradeReporter(Base):
    __tablename__ = 'comtrade_reporter'
    __table_args__ = {
        'schema': 'public',
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
        'schema': 'public',
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
        'schema': 'public',
        'comment': 'Справочник CMD:H6 из Comtrade'
    }
    id: Mapped[int_pk]
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]
    is_active: Mapped[is_active]
    foreign_id: Mapped[str | None] = mapped_column(comment='Код товарной категории')
    text: Mapped[str | None] = mapped_column(Text, comment='Описание товарной категории')
    parent: Mapped[str | None] = mapped_column(comment='Входит в группу')
