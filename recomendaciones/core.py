from .helpers import exec_query, make_dataframe
import pandas as pd
from pandas import DataFrame
from datetime import datetime, timedelta


def eliminar_registros(fi, ff, conn):
    delete_from_recom_implemented = """
    DELETE from recom_implemented ri WHERE ri.created_at BETWEEN '{fi}' and '{ff}'
    """.format(fi=fi, ff=ff)

    exec_query(query=delete_from_recom_implemented, conn=conn)


def obtener_recom_profit(fi, ff, conn):
    sql_recom_profit = """
        select
            rlv.id,
            rlv."date",
            rlv.created_at,
            rlv.updated_at,
            rlv."user",
            rlv.actual_value,
            rlv.recommended_value,
            rlv."comments",
            rlv.feedback,
            rlv.descripcion,
            rlv.modelo,
            case
                when rlv.tag = 'TPH_SAG1' then 'SAG:WIC2101'
                when rlv.tag = 'TPH_SAG2' then 'sag2:260_wit_1835'
            else rlv.tag end as tag,
            rlv.driving_factor2 
        from
            recommendations_limits_view2 rlv
        where
            rlv.driving_factor2 = 'Profit' and
            rlv.updated_at is not null and
            rlv.feedback = 1 and
            rlv.created_at between '{fi}' and '{ff}'
            order by rlv.created_at asc;
    """.format(fi=fi, ff=ff)
    df = make_dataframe(conn=conn, query=sql_recom_profit)
    return df


def obtener_recom_setpoint(fi, ff, conn):
    sql_recom_setpoint = """
                select
                    rlv.id,
                    rlv."date",
                    rlv.created_at,
                    rlv.updated_at,
                    rlv."user",
                    rlv.actual_value,
                    rlv.recommended_value,
                    rlv."comments",
                    rlv.feedback,
                    rlv.descripcion,
                    rlv.modelo,
                    rlv.tag,
                    rlv.driving_factor2
                from
                    recommendations_limits_view2 rlv
                where
                    rlv.driving_factor2 = '' and
                    rlv.updated_at is not null and
                    rlv.feedback = 1 and
                    rlv.created_at between '{fi}' and '{ff}'
                    order by rlv.created_at asc;
                """.format(fi=fi, ff=ff)

    df = make_dataframe(conn=conn, query=sql_recom_setpoint)
    return df


def obtener_datos_por_minutos(process_conn , created_at, tag, hora_fin_turno, driving_factor):
    process_data_sql = """
            select
            DISTINCT t.\"TimeStamp\",
            ll.value as ll,
            hl.value as hl,
            t.value as value
        FROM
            public.limits_tags l
            left join input_tags t on t.tag = l.tag
            left join input_tags ll on ll.tag = l.tagll
            and t.\"TimeStamp\" = ll.\"TimeStamp\"
            left join input_tags hl on hl.tag = l.taghl
            and t.\"TimeStamp\" = hl.\"TimeStamp\"
        where
            l.tag = '{tag}'
            and t.\"TimeStamp\" BETWEEN timestamp '{created_at}'
            AND TIMESTAMP '{hora_fin_turno}'
        order by
            t.\"TimeStamp\" asc;
        """.format(created_at=created_at, tag=tag, hora_fin_turno=hora_fin_turno)

    df_process = make_dataframe(conn=process_conn, query=process_data_sql)
    if driving_factor != '':
        df_process = df_process.dropna()

    return df_process


def obtener_datos_por_minutos_turno_anterior(process_conn , created_at, tag, driving_factor):
    process_data_sql = """
            select
            DISTINCT t.\"TimeStamp\",
            ll.value as ll,
            hl.value as hl,
            t.value as value
        FROM
            public.limits_tags l
            left join input_tags t on t.tag = l.tag
            left join input_tags ll on ll.tag = l.tagll
            and t.\"TimeStamp\" = ll.\"TimeStamp\"
            left join input_tags hl on hl.tag = l.taghl
            and t.\"TimeStamp\" = hl.\"TimeStamp\"
        where
            l.tag = '{tag}'
            and t.\"TimeStamp\" BETWEEN timestamp '{created_at}' - INTERVAL '15m'
            AND TIMESTAMP '{created_at}'
        order by
            t.\"TimeStamp\" asc;
        """.format(created_at=created_at, tag=tag)

    df_process = make_dataframe(conn=process_conn, query=process_data_sql)

    if driving_factor != '':
        df_process = df_process.dropna()

    return df_process


def get_df_resultado(val_actual, val_recom, created_at, updated_at, tag_hl, tag_ll, setpoint, usuario, description,
                     tag, modelo, parcial, total, entre_limites):

    df = pd.DataFrame(columns=['val_actual', 'val_recom', 'created_at', 'updated_at', 'tag_hl', 'tag_ll', 'setpoint',
                          'usuario', 'description', 'tag', 'parcial', 'total', 'entre_limites', 'modelo'])

    df = df.append({'val_actual': val_actual, 'val_recom': val_recom, 'created_at': created_at, 'updated_at': updated_at,
               'tag_hl': tag_hl, 'tag_ll': tag_ll, 'setpoint': setpoint, 'usuario': usuario, 'description': description,
               'tag': tag, 'parcial': parcial, 'total': total, 'entre_limites': entre_limites, 'modelo': modelo},
                   ignore_index=True)

    return df


def respuesta_caducada(created_at, updated_at, tiempo_respuesta):
    fecha_caducada = (created_at + timedelta(hours=tiempo_respuesta))
    if updated_at >= fecha_caducada:
        return True
    else:
        return False


def recom_hl_o_ll(hl, ll, val_recom):
    recom = ''
    if val_recom > hl:
        recom = 'hl'
    elif val_recom < ll:
        recom = 'll'
    elif val_recom <= hl or val_recom >= ll:
        recom = 'entre_limites'
    return recom


def tipo_de_implementacion(df: "DataFrame", val_recom, tipo_recom):

    if tipo_recom == 'hl':
        hl_actual = df.iloc[0]['hl']

        ganancia_esperada = abs(hl_actual - val_recom) * (50 / 100)
        implementacion_parcial = ganancia_esperada + hl_actual

        total_hl = df.query('hl >= @val_recom')
        parcial_hl = df.query('hl >= @implementacion_parcial')

        if not total_hl.empty:
            return 'total'
        elif not parcial_hl.empty:
            return 'parcial'
        return ''
    elif tipo_recom == 'll':
        ll_actual = df.iloc[0]['ll']

        ganancia_esperada = abs(ll_actual - val_recom) * (50 / 100)
        implementacion_parcial = (ll_actual - ganancia_esperada)

        total_ll = df.query('ll <= @val_recom')
        parcial_ll = df.query('ll <= @implementacion_parcial')

        if not total_ll.empty:
            return 'total'
        elif not parcial_ll.empty:
            return 'parcial'
        return ''


def se_matuvo(df: "DataFrame", val_recom, tipo_recom, tipo_implementacion, valor_esperado):
    formato = "%Y-%m-%d %H:%M:%S"
    if tipo_recom == 'hl':
        if tipo_implementacion == 'parcial':
            timestamp_valor_alcanzado = df.query('@hl >= @valor_esperado').iloc[0, 0]
        else:
            timestamp_valor_alcanzado = df.query('@hl >= @val_recom').iloc[0, 0]
        timestamp_esperado = timestamp_valor_alcanzado + timedelta(hours=1)
        (ll, hl, value) = df.query(
            'TimeStamp >= @timestamp_valor_alcanzado & TimeStamp <= @timestamp_esperado').mean().round()
        if hl >= val_recom:
            return True
    elif tipo_recom == 'll':
        if tipo_implementacion == 'parcial':
            timestamp_valor_alcanzado = df.query('valor_esperado <= @val_recom').iloc[0, 0]
        else:
            timestamp_valor_alcanzado = df.query('ll <= @val_recom').iloc[0, 0]
        timestamp_esperado = timestamp_valor_alcanzado + datetime.timedelta(hours=1)
        (ll, hl, value) = df.query(
            'TimeStamp >= @timestamp_valor_alcanzado & TimeStamp <= @timestamp_esperado').mean().round()
        if ll <= val_recom:
            return True
    else:
        return False