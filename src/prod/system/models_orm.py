from sqlalchemy import text, DDL, event

from src.prod.system.database import session_sync
from src.prod.system.models import ComtradeReturn


#  не работает ! надо через класс DDL

def OrmCreatSystem():
    sql_function = """
        CREATE OR REPLACE FUNCTION public.comtrade_return_update_hash_address()
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
    """
    sql_trigger = """        
        CREATE TRIGGER comtrade_return_update_hash_address_TRIGGER
        BEFORE INSERT or update on comtrade_return
        FOR EACH ROW
        EXECUTE PROCEDURE comtrade_return_update_hash_address();
        """

    sql_index_hash_address = """
    CREATE INDEX 
        comtrade_return_hash_address_idx ON comtrade_return
    USING gin (hash_address);
                            """

    with session_sync() as session:
        session.execute(text(sql_function))
        session.execute(text(sql_trigger))
        #session.execute(text(sql_index_hash_address))

