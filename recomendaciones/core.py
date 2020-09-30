from .helpers import exec_query, make_dataframe
import pandas as pd


def eliminar_registros(fi, ff, conn):
    delete_from_recom_implemented = """
    DELETE from recom_implemented ri WHERE ri.created_at BETWEEN '{fi}' and '{ff}'
    """.format(fi=fi, ff=ff)

    exec_query(query=delete_from_recom_implemented, conn=conn)


def obtener_recom_profit(fi, ff, conn):
    sql_recom_profit = """
              SELECT
                o.id,
                o. "Usuario",
                CASE WHEN o.setpoint = 'tph_MUN' THEN
                    'MU:280_WIC_8778'
                    WHEN setpoint = 'tph_M12' THEN
                    'MOL:WIC44N_MINERAL'
                    WHEN setpoint = 'tph_M11' THEN
                    'MOL:WIC44M_MINERAL'
                    WHEN setpoint = 'tph_M10' THEN
                    'MOL:WIC44L_MINERAL'
                    WHEN setpoint = 'tph_M9' THEN
                    'MOL:WIC44K_MINERAL'
                    WHEN setpoint = 'tph_M8' THEN
                    'MOL:WIC44J_MINERAL'
                    WHEN setpoint = 'tph_M7' THEN
                    'MOL:WIC44G_MINERAL'
                    WHEN setpoint = 'tph_M6' THEN
                    'MOL:WIC44F_MINERAL'
                    WHEN setpoint = 'tph_M5' THEN
                    'MOL:WIC44E_MINERAL'
                    WHEN setpoint = 'tph_M4' THEN
                    'MOL:WIC44D_MINERAL'
                    WHEN setpoint = 'tph_M3' THEN
                    'MOL:WIC44C_MINERAL'
                    WHEN setpoint = 'tph_M2' THEN
                    'MOL:WIC44B_MINERAL'
                    WHEN setpoint = 'tph_M1' THEN
                    'MOL:WIC44A_MINERAL'
                ELSE
                    o.setpoint
                END AS "Tag",
                o.actual_value as "Actual_Value",
                o.recommended_value as "Recommended_Value",
                o.created_at,
                o.updated_at - INTERVAL '3h' as updated_at,
                o.id_recom,
                o. "Driving_Factor",
                o.n_molino,
                l.description
            FROM
                "mcmc_recommendations_ALL" o
                LEFT JOIN limits_tags l ON l.tag = o.setpoint
            WHERE
                o.created_at BETWEEN '{fi}'
                AND '{ff}'
                AND o. "Driving_Factor" != 'SP'
                AND o.feedback = 1
            ORDER BY
                created_at ASC;
                """.format(fi=fi, ff=ff)
    df = make_dataframe(conn=conn, query=sql_recom_profit)
    return df


def obtener_recom_setpoint(fi, ff, conn):
    sql_recom_setpoint = """
                SELECT
                o. "Usuario",
                o.setpoint as "Tag",
                o.actual_value as "Actual_Value",
                o.recommended_value as "Recommended_Value",
                o.created_at,
                o.updated_at - INTERVAL '3h' as updated_at,
                o.id_recom,
                o. "Driving_Factor",
                o.n_molino,
                l.description
            FROM
                "mcmc_recommendations_ALL" o
                LEFT JOIN limits_tags l ON l.tag = o.setpoint
            WHERE
                o.created_at BETWEEN '{fi}'
                AND '{ff}'
                AND o. "Driving_Factor" = 'SP'
                AND o.feedback = 1
            ORDER BY
                created_at ASC;
                """.format(fi=fi, ff=ff)

    df = make_dataframe(conn=conn, query=sql_recom_setpoint)
    return df


def obtener_datos_por_minutos(process_conn , created_at, tag, driving_factor):
    process_data_sql = """
            select
            DISTINCT t."timestamp",
            ll.value as ll,
            hl.value as hl,
            t.value as value
        FROM
            public.limits_tags l
            left join input_tags t on t.tag = l.tag
            left join input_tags ll on ll.tag = l.tagll
            and t."timestamp" = ll."timestamp"
            left join input_tags hl on hl.tag = l.taghl
            and t."timestamp" = hl."timestamp"
        where
            l.tag = '{tag}'
            and t."timestamp" BETWEEN timestamp '{created_at}'
            AND TIMESTAMP '{created_at}' + INTERVAL '2 hours'
        order by
            t."timestamp" asc;
        """.format(created_at=created_at, tag=tag)

    df_process = make_dataframe(conn=process_conn, query=process_data_sql)
    if driving_factor != 'SP':
        df_process = df_process.dropna()

    return df_process


def obtener_datos_por_minutos_turno_anterior(process_conn , created_at, tag, driving_factor):
    process_data_sql = """
            select
            DISTINCT t."timestamp",
            ll.value as ll,
            hl.value as hl,
            t.value as value
        FROM
            public.limits_tags l
            left join input_tags t on t.tag = l.tag
            left join input_tags ll on ll.tag = l.tagll
            and t."timestamp" = ll."timestamp"
            left join input_tags hl on hl.tag = l.taghl
            and t."timestamp" = hl."timestamp"
        where
            l.tag = '{tag}'
            and t."timestamp" BETWEEN timestamp '{created_at}' - INTERVAL '15m'
            AND TIMESTAMP '{created_at}'
        order by
            t."timestamp" asc;
        """.format(created_at=created_at, tag=tag)

    df_process = make_dataframe(conn=process_conn, query=process_data_sql)

    if driving_factor != 'SP':
        df_process = df_process.dropna()

    return df_process


def get_df_resultado(val_actual, val_recom, created_at, updated_at, tag_hl, tag_ll, setpoint, usuario, description,
                     tag, n_molino, parcial, total, entre_limites):

    df = pd.DataFrame(columns=['val_actual', 'val_recom', 'created_at', 'updated_at', 'tag_hl', 'tag_ll', 'setpoint',
                          'usuario', 'description', 'tag', 'parcial', 'total', 'entre_limites', 'n_molino'])

    df = df.append({'val_actual': val_actual, 'val_recom': val_recom, 'created_at': created_at, 'updated_at': updated_at,
               'tag_hl': tag_hl, 'tag_ll': tag_ll, 'setpoint': setpoint, 'usuario': usuario, 'description': description,
               'tag': tag, 'parcial': parcial, 'total': total, 'entre_limites': entre_limites, 'n_molino': n_molino},
                   ignore_index=True)

    return df
