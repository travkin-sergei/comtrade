"""
Модель данных приложения.

Содержит комментарии к таблицам и столбцам в формате JSON.
В базовый класс имеет методы:
    save
    set
    get_all
"""
from contextlib import contextmanager
from sqlalchemy import (
    and_,
    MetaData,
    Date,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    true,
)
from sqlalchemy.orm import mapped_column, declarative_base
from comtrade.config import session_sync, logg


@contextmanager
def session_scope():
    session = session_sync()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


class Base(declarative_base(metadata=MetaData(schema="comtrade"))):
    """Абстрактный класс."""

    __abstract__ = True

    def save(self):
        with session_scope() as session:
            session.add(self)
            logg.info(f"Объект класса {self.__class__.__name__} успешно сохранен.")

    @classmethod
    def set(cls, json_data: dict):
        obj_hash_address = json_data.get('hash_address')
        class_name = cls.__name__

        session = session_sync()
        try:
            obj = session.query(cls).filter_by(hash_address=obj_hash_address).first()

            if obj is None:
                logg.warning(f"Объект с hash_address={obj_hash_address} не найден.")
                return

            for field, value in json_data.items():
                if hasattr(obj, field):
                    setattr(obj, field, value)
                    logg.info(f"Поле {field} обновлено на {value}.")
                else:
                    logg.warning(f"Поле {field} не найдено в классе {class_name}. Пропускаем.")

            session.commit()
            logg.info(f"Объект класса {class_name} с hash_address={obj_hash_address} успешно обновлен.")
        except Exception as error:
            session.rollback()
            logg.error(f"Ошибка при обновлении объекта класса {class_name}: {error}")
            raise RuntimeError(f"Ошибка при обновлении объекта: {error}")
        finally:
            session.close()

    @classmethod
    def get_all(cls, filters: dict = None, batch_size: int = 50):
        class_name = cls.__name__
        session = session_sync()
        try:
            query = session.query(cls)
            if filters:
                conditions = []
                for key, value in filters.items():
                    if isinstance(value, list):
                        conditions.append(getattr(cls, key).in_(value))
                    else:
                        conditions.append(getattr(cls, key) == value)
                query = query.filter(and_(*conditions))

            # Используем yield_per для извлечения порциями
            for obj in query.yield_per(batch_size):
                yield obj
        except Exception as error:
            logg.error(f"Ошибка при извлечении объектов класса {class_name}: {error}")
            return []
        finally:
            session.close()


class Key(Base):
    __tablename__ = 'settings_key'
    __table_args__ = {
        'comment':
            '{"name":"Список зарегистрированных ключей.",'
            '"description":"Периодически требуется менять",}'
    }
    id = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    created_at = mapped_column(DateTime, server_default=func.now(), comment='{"name":"Запись создана"}')
    updated_at = mapped_column(
        DateTime, server_default=func.now(), server_onupdate=func.now(), comment='{"name":"Запись обновлена"}'
    )
    hash_address = mapped_column(
        String,
        nullable=True,
        comment='{"name":"хеш сумма адреса строки",'
                '"description":"key"}'
    )
    is_active = mapped_column(Boolean, server_default=true(), nullable=False, comment='{"name":"Запись активна"}')
    key = mapped_column(String, comment='{"name":"Ключ"}')
    status = mapped_column(String, comment='{"name":"Статус код запросов для ключа."}')
    email = mapped_column(String, comment='{"name":"Зарегистрирована на почту"}')
    password = mapped_column(String, comment='{"name":"Пароль."}')


class VersionDirectory(Base):
    __tablename__ = 'settings_version_directory'
    __table_args__ = {
        #  "schema": "comtrade",
        'comment':
            '{"name":"Хеш суммы справочников",'
            '"description":"Необходимо для быстрой проверки справочников первоисточника на изменение справочников.",}'
    }
    id = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    created_at = mapped_column(DateTime, server_default=func.now(), comment='{"name":"Запись создана"}')
    updated_at = mapped_column(
        DateTime, server_default=func.now(), server_onupdate=func.now(), comment='{"name":"Запись обновлена"}'
    )
    is_active = mapped_column(Boolean, server_default=true(), nullable=False, comment='{"name":"Запись активна"}')

    hash_address = mapped_column(
        String,
        nullable=True,
        comment='{"name":"хеш сумма адреса строки",'
                '"description":"cmd_code,hs"}'
    )
    alias = mapped_column(String, comment='{"name":"Название таблицы во внешней системе"}')
    table_name = mapped_column(String, comment='{"name":"Название таблицы в безе данных"}')
    tab_hash = mapped_column(String, comment='{"name":"хеш сумма таблицы"}')


class VersionData(Base):
    """Версия данных."""

    __tablename__ = 'load_version_data'
    __table_args__ = {
        #  "schema": "comtrade",
        'comment':
            '{"name":"Версия данных",'
            '"npa":"https://comtradeapi.un.org/public/v1/getDA/C/{A или M}/HS",}'
    }

    id = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    created_at = mapped_column(DateTime, server_default=func.now(), comment='{"name":"Запись создана"}')
    updated_at = mapped_column(
        DateTime, server_default=func.now(), server_onupdate=func.now(), comment='{"name":"Запись обновлена"}'
    )
    is_active = mapped_column(Boolean, server_default=true(), nullable=False, comment='{"name":"Запись активна"}')
    hash_address = Column(
        String,
        nullable=True,
        comment='{"name":"хеш сумма адреса строки"'
                '"description":"reporter_code,dataset_code",}'
    )
    dataset_code = mapped_column(BigInteger, nullable=False)
    type_code = mapped_column(
        String, nullable=True,
        comment='{"name":"Код типа HS",'
                '"description":"C для товаров и S для услуг"}'
    )
    freq_code = mapped_column(
        String, nullable=True,
        comment='{"name":"Код типа дат",'
                '"description":"A для годовых и M для ежемесячных"}'
    )
    period = mapped_column(
        String, nullable=True,
        comment='{"name":"период",'
                '"description":"года месяц (для месячных), год для (годовых)"}'
    )
    reporter_code = mapped_column(Integer, nullable=True, comment='{"name":"Страна или географический район"}')
    reporter_iso = mapped_column(String, nullable=True, comment='{"name":"ISO 3 код репортера"}')
    reporter_desc = mapped_column(String, nullable=True, comment='{"name":"наименование репортера"}')
    classification_code = mapped_column(String, nullable=True, comment='{"name":"версия классификатора (HS, SITC)."}')
    classification_search_code = mapped_column(
        String, nullable=True,
        comment='{"name":"представлена ли классификация по странам или нет"}'
    )
    is_original_classification = mapped_column(
        Boolean, nullable=True, comment='{"name":"это оригинальная классификация"}'
    )
    is_extended_flow_code = mapped_column(Boolean, nullable=True)
    is_extended_partner_code = mapped_column(Boolean, nullable=True)
    is_extended_partner2_code = mapped_column(Boolean, nullable=True)
    is_extended_cmd_code = mapped_column(Boolean, nullable=True)
    is_extended_customs_code = mapped_column(Boolean, nullable=True)
    is_extended_mot_code = mapped_column(Boolean, nullable=True)
    total_records = mapped_column(Integer, nullable=True, comment='{"name":"количество записей"}')
    dataset_checksum = mapped_column(BigInteger, nullable=True, comment='{"name":"хеш сумма данных"}')
    first_released = mapped_column(DateTime, nullable=True, comment='{"name":"дата публикации"}')
    last_released = mapped_column(DateTime, nullable=True, comment='{"name":"дата обновления"}')


class PartnerAreas(Base):
    __tablename__ = 'load_partner_areas'
    __table_args__ = {
        'comment':
            '{"name":"Справочник территорий (partnerAreas)",'
            '"npa":"https://comtradeapi.un.org/files/v1/app/reference/partnerAreas.json",}'
    }

    id = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    created_at = mapped_column(DateTime, server_default=func.now(), comment='{"name":"Запись создана"}')
    updated_at = mapped_column(
        DateTime, server_default=func.now(), server_onupdate=func.now(), comment='{"name":"Запись обновлена"}'
    )
    is_active = mapped_column(Boolean, server_default=true(), nullable=False, comment='{"name":"Запись активна"}')
    hash_address = mapped_column(
        String,
        nullable=True,
        comment='{"name":"хеш сумма адреса строки",'
                '"description":"foreign_id",}'
    )
    foreign_id = mapped_column(Integer, nullable=False, comment='{"name":"код территории"}')
    text = mapped_column(String, nullable=False)
    partner_code = mapped_column(
        Integer, nullable=True,
        comment='{"name":"Основная страна-партнер или географический район для соответствующего торгового потока"}'
    )
    partner_desc = mapped_column(String, nullable=True)
    partner_note = mapped_column(String, nullable=True)
    partner_code_iso_alpha2 = mapped_column(String, nullable=True)
    partner_code_iso_alpha3 = mapped_column(String, nullable=True)
    entry_effective_date = mapped_column(DateTime, nullable=True)
    is_group = mapped_column(Boolean, nullable=False)


class HS(Base):
    """Список кодов гармонизированной системы Comtrade. Анализ показал, что он не полный."""

    __tablename__ = 'load_hs'
    __table_args__ = {
        'comment':
            '{"name":"Справочник гармонизированой системы (HS)", '
            '"description":"От H0 далее перебираем Н1,Н2, ...",'
            '"npa": "https://comtradeapi.un.org/files/v1/app/reference/H0.json", }'
    }

    id = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    created_at = mapped_column(DateTime, server_default=func.now(), comment='{"name":"Запись создана"}')
    updated_at = mapped_column(
        DateTime, server_default=func.now(), server_onupdate=func.now(), comment='{"name":"Запись обновлена"}'
    )
    is_active = mapped_column(Boolean, server_default=true(), nullable=False, comment='{"name":"Запись активна"}')
    hash_address = Column(
        String,
        nullable=True,
        comment='{"name":"хеш сумма адреса строки"'
                '"description":"cmd_code,hs",}'
    )

    classification_code = mapped_column(String, nullable=True, comment='{"name":"Кодировка HS"}')
    cmd_code = mapped_column(String, nullable=True, comment='{"name":"Код товарной категории"}')
    text = mapped_column(Text, nullable=True, comment='{"name":"Описание товарной категории"}')
    parent = mapped_column(String, nullable=True, comment='{"name":"Входит в группу"}')
    is_leaf = mapped_column(Integer, nullable=True, )
    aggr_level = mapped_column(Integer, nullable=True, comment='{"name":"уровень вложенности"}')
    standard_unit_abbr = mapped_column(
        String, nullable=True,
        comment='{"name":"стандартные сокращения единиц измерения"}'
    )


class TradeRegimes(Base):
    """торговые режимы"""

    __tablename__ = 'load_trade_regimes'

    __table_args__ = {
        'comment':
            '{"name":"торговые режимы",'
            '"description":"Код торгового потока или подток (экспорт, реэкспорт, импорт, повторный импорт и т.д.)", '
            '"npa":"https://comtradeapi.un.org/files/v1/app/reference/tradeRegimes.json",}'
    }
    id = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    created_at = mapped_column(DateTime, server_default=func.now(), comment='{"name":"Запись создана"}')
    updated_at = mapped_column(
        DateTime, server_default=func.now(), server_onupdate=func.now(), comment='{"name":"Запись обновлена"}'
    )
    is_active = mapped_column(Boolean, server_default=true(), nullable=False, comment='{"name":"Запись активна"}')
    hash_address = mapped_column(
        String,
        nullable=True,
        comment='{"name":"хеш сумма адреса строки",'
                ' "description":"flow_code",}'
    )
    flow_code = mapped_column(String, nullable=False, comment='{"name":"Код потока"}')
    flow_desc = mapped_column(String, nullable=False, comment='{"name":"Расшифровка кода торгового потока"}')


class TransportCodes(Base):
    """Виды транспорта."""

    __tablename__ = 'load_transport_codes'
    __table_args__ = {
        'comment':
            '{"name":"Виды транспорта.",'
            '"description":"Вид транспорта, используемый, при перемещении товаров", '
            '"npa":"https://comtradeapi.un.org/files/v1/app/reference/ModeOfTransportCodes.json",}'
    }
    id = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    created_at = mapped_column(DateTime, server_default=func.now(), comment='{"name":"Запись создана"}')
    updated_at = mapped_column(
        DateTime, server_default=func.now(), server_onupdate=func.now(), comment='{"name":"Запись обновлена"}'
    )
    is_active = mapped_column(Boolean, server_default=true(), nullable=False, comment='{"name":"Запись активна"}')
    hash_address = mapped_column(
        String,
        nullable=True,
        comment='{"name":"хеш сумма адреса строки",'
                ' "description":"foreign_id",}'
    )
    foreign_id = mapped_column(String, nullable=False, comment='{"name":"Код транспорта"}')
    text = mapped_column(String, nullable=False, comment='{"name":"Расшифровка кода транспорта"}')


class CustomsCodes(Base):
    """Код таможенной процедуры."""

    __tablename__ = 'load_customs_codes'
    __table_args__ = {
        'comment':
            '{"name":"Код таможенной процедуры.",'
            '"description":"Таможенная или статистическая процедура.", '
            '"npa":"https://comtradeapi.un.org/files/v1/app/reference/CustomsCodes.json",}'
    }
    id = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    created_at = mapped_column(DateTime, server_default=func.now(), comment='{"name":"Запись создана"}')
    updated_at = mapped_column(
        DateTime, server_default=func.now(), server_onupdate=func.now(), comment='{"name":"Запись обновлена"}'
    )
    is_active = mapped_column(Boolean, server_default=true(), nullable=False, comment='{"name":"Запись активна"}')
    hash_address = mapped_column(
        String,
        nullable=True,
        comment='{"name":"хеш сумма адреса строки",'
                ' "description":"foreign_id",}'
    )
    foreign_id = mapped_column(String, nullable=False, comment='{"name":"Код таможенной процедуры."}')
    text = mapped_column(String, nullable=False, comment='{"name":"Расшифровка кода таможенной процедуры"}')


class WorldStatistic(Base):
    """Статистика международной торговли."""

    __tablename__ = 'load_world_statistic'
    __table_args__ = (
        UniqueConstraint('hash_address'),
        {
            #  "schema": "comtrade",
            'comment':
                '{"name":"Статистика международной торговли.",'
                '"npa":"https://comtradeapi.un.org/data/v1/get/{typeCode}/{freqCode}/HS?...",}'
        }
    )

    id = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    created_at = mapped_column(DateTime, server_default=func.now(), comment='{"name":"Запись создана"}')
    updated_at = mapped_column(
        DateTime, server_default=func.now(), server_onupdate=func.now(), comment='{"name":"Запись обновлена"}'
    )
    is_active = mapped_column(Boolean, server_default=true(), nullable=False, comment='{"name":"Запись активна"}')
    hash_address = mapped_column(
        String(64),
        comment='{"name":"хеш сумма адреса строки",'
                '"description":"last_released+period+flow_code+cmd_code+reporter_code+partner_code+partner2_code",}'
    )
    type_code = mapped_column(String, comment='{"name":"Код типа: C для товаров и S для услуг"}')
    freq_code = mapped_column(String, comment='{"name":"Код типа: A для годовых и M для ежемесячных"}')

    ref_year = mapped_column(Integer, comment='{"name":"Период наблюдения"}')
    ref_month = mapped_column(
        Integer,
        comment='{"name":"Месяц наблюдения. Для годового значения было бы установлено значение 52"}'
    )

    period = mapped_column(String, comment='{"name":"Статистика за период"}')
    reporter_code = mapped_column(Integer, comment='{"name":"Страна или географический район"}')
    reporter_iso = mapped_column(String, comment='{"name":"ISO 3 код репортера"}')
    flow_code = mapped_column(
        String,
        comment='{"name":"Торговый поток или подпоток (экспорт, реэкспорт, импорт, повторный импорт и т.д.)"}'
    )
    # flow_desc = mapped_column(String, comment='{"name":"Описание торговых потоков"}')
    partner_code = mapped_column(
        Integer,
        comment='{"name":"Основная страна-партнер или географический район для соответствующего торгового потока"}'
    )
    partner_iso = mapped_column(String, comment='{"name":"ISO 3 код 1-го партнера"}')
    # partner_desc = mapped_column(String, comment='{"name":"Описание 1-го партнера"}')
    partner2_code = mapped_column(
        Integer,
        comment='{"name":"Второстепенная страна-партнер или географический район для соответствующего торгового потока"}'
    )
    partner2_iso = mapped_column(String, comment='{"name":"Код ISO 3 2-го партнера"}')
    # partner2_desc = mapped_column(String, comment='{"name":"Описание 2-го партнера"}')
    classification_code = mapped_column(String, comment='{"name":"версия классификатора (HS, SITC)."}')
    classification_search_code = mapped_column(
        String,
        comment='{"name":"Флажок, указывающий, представлена ли классификация по странам или нет"}'
    )
    is_original_classification = mapped_column(Boolean)
    cmd_code = mapped_column(String, comment='{"name":"Код продукта в сочетании с классификационным кодом"}')
    cmd_desc = mapped_column(String, comment='{"name":"Описание категории товара/услуги"}')
    aggr_level = mapped_column(
        Integer,
        comment='{"name":"Иерархический уровень категории товара/услуги",'
                '"description":"длинна поля cmd_code 2,4,6,8,10",}'
    )
    is_leaf = mapped_column(
        Boolean,
        comment='{"name":"Определение того, имеет ли код товара самый базовый уровень (т.е. подзаголовок для ТН ВЭД)"}'
    )
    customs_code = mapped_column(String, comment='{"name":"Таможенная или статистическая процедура"}')
    # customs_desc = mapped_column(String, comment='{"name":"Описание таможенной процедуры"}')
    mos_code = mapped_column(
        String,
        comment='{"name":"Способ поставки при оказании услуг (только торговля услугами)"}'
    )
    mot_code = mapped_column(
        String,
        comment='{"name":"Вид транспорта",'
                '"description":".используемый вид транспорта, '
                'когда товары въезжают на экономическую территорию страны или покидают ее",}'
    )
    # mot_desc = mapped_column(String, comment='{"name":"Описание вида транспорта"}')
    qty_unit_code = mapped_column(
        Integer,
        comment='{"name":"Единица первичного количества",'
                '"description":"значение из справочника",}'
    )
    qty_unit_abbr = mapped_column(String, comment='{"name":"Аббревиатура первичной единицы измерения количества"}')
    qty = Column(Numeric, comment='{"name":"Значение первичного количества"}')
    is_qty_estimated = mapped_column(
        Boolean,
        comment='{"name":"Отметьте, подсчитано ли первичное количество или нет"}'
    )
    alt_qty_unit_code = mapped_column(Integer, comment='{"name":"Единица вторичного количества"}')
    alt_qty_unit_abbr = mapped_column(String, comment='{"name":"Аббревиатура вторичной единицы измерения количества"}')
    alt_qty = mapped_column(Numeric, comment='{"name":"Значение вторичной величины"}')
    is_alt_qty_estimated = mapped_column(
        Boolean,
        comment='{"name":"Отметьте, подсчитано ли вторичное количество или нет"}'
    )
    net_wgt = mapped_column(Numeric, comment='{"name":"Вес нетто"}')
    is_net_wgt_estimated = mapped_column(
        Boolean, comment='{"name":"Отметьте, указан ли расчетный вес нетто или нет"}'
    )
    gross_wgt = mapped_column(Numeric, comment='{"name":"Вес брутто"}')
    is_gross_wgt_estimated = mapped_column(
        Boolean,
        comment='{"name":"Отметьте, указан ли расчетный вес брутто или нет"}'
    )
    cif_value = mapped_column(Numeric, comment='{"name":"Стоимость импорта CIF"}')
    fob_value = mapped_column(Numeric, comment='{"name":"Стоимость экспорта FOB"}')
    primary_value = mapped_column(
        Numeric,
        comment='{"name":"Первичные торговые значения (взятые из значений CIF или FOB)"}'
    )
    legacy_estimation_flag = mapped_column(String, comment='{"name":"Флаг оценки унаследованного количества"}')
    is_reported = mapped_column(Boolean, comment='{"name":"Флажок, указывающий, представлена ли запись по стране"}')
    is_aggregate = mapped_column(Boolean, comment='{"name":"Флаг, указывающий, агрегируется ли запись с помощью UNSD"}')
    dataset_checksum = mapped_column(
        Integer,
        comment='{"name":"дата обновления", '
                '"description":"покаждой стране за отпределенный приод уникальная",}'
    )


class PlanRequest(Base):
    """План запроса."""

    __tablename__ = 'record_plan_request'
    __table_args__ = {
        #  "schema": "comtrade",
        'comment':
            '{"name":"План запроса",'
            '"description":"Здесь хранится план запроса",}'
    }
    id = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    created_at = mapped_column(DateTime, server_default=func.now(), comment='{"name":"Запись создана"}')
    updated_at = mapped_column(
        DateTime, server_default=func.now(), server_onupdate=func.now(), comment='{"name":"Запись обновлена"}'
    )
    is_active = mapped_column(Boolean, server_default=true(), nullable=False, comment='{"name":"Запись активна"}')

    dataset_checksum = mapped_column(
        BigInteger,
        comment='{"name":"хеш сумма данных",'
                '"description":"покаждой стране за отпределенный приод уникальная"}'
    )
    params = mapped_column(String, nullable=False, comment='{"name":"Параметры запроса"}')
    status_code = mapped_column(Integer, nullable=False, comment='{"name":"Статус код get запроса"}')
    count_row = mapped_column(Integer, nullable=False, comment='{"name":"Количетсво строк ответа"}')
