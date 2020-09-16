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
                o.updated_at - INTERVAL '4h' as updated_at,
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
                o.updated_at - INTERVAL '4h' as updated_at,
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


def insert_into_implementadas_profit(actual_value, recom_value, created_at, updated_at, usuario, tag, description,
                                     hl_actual, ll_actual, n_molino, output_conn,
                                     entre_limites=False, total=False, parcial=False):
    if entre_limites:
        insert_into_recomimplemented = """
                            insert INTO recom_implemented(
                            val_actual,
                            val_recom,
                            created_at,
                            updated_at,
                            usuario,
                            setpoint,
                            description,
                            tag_hl,
                            tag_ll,
                            entre_limites,
                            n_molino)
                            VALUES ({0}, {1},'{2}','{3}','{4}',
                                    '{5}','{6}',{7},{8},{9},{10});
                            """.format(actual_value,
                                       recom_value,
                                       created_at,
                                       updated_at,
                                       usuario,
                                       tag,
                                       description,
                                       hl_actual,
                                       ll_actual,
                                       entre_limites,
                                       n_molino)

    if total:
        insert_into_recomimplemented = """
                                    insert INTO recom_implemented(
                                    val_actual,
                                    val_recom,
                                    created_at,
                                    updated_at,
                                    usuario,
                                    setpoint,
                                    description,
                                    tag_hl,
                                    tag_ll,
                                    total,
                                    n_molino)
                                    VALUES ({0}, {1},'{2}','{3}','{4}',
                                            '{5}','{6}',{7},{8},{9},{10});
                                    """.format(actual_value,
                                               recom_value,
                                               created_at,
                                               updated_at,
                                               usuario,
                                               tag,
                                               description,
                                               hl_actual,
                                               ll_actual,
                                               total,
                                               n_molino)

    if parcial:
        insert_into_recomimplemented = """
                                        insert INTO recom_implemented(
                                        val_actual,
                                        val_recom,
                                        created_at,
                                        updated_at,
                                        usuario,
                                        setpoint,
                                        description,
                                        tag_hl,
                                        tag_ll,
                                        parcial,
                                        n_molino)
                                        VALUES ({0}, {1},'{2}','{3}','{4}',
                                                '{5}','{6}',{7},{8},{9},{10});
                                        """.format(actual_value,
                                                   recom_value,
                                                   created_at,
                                                   updated_at,
                                                   usuario,
                                                   tag,
                                                   description,
                                                   hl_actual,
                                                   ll_actual,
                                                   parcial,
                                                   n_molino)

    exec_query(conn=output_conn, query=insert_into_recomimplemented)

def insert_into_implementadas_no_profit(output_conn, val_actual, val_recom, created_at, updated_at,
                                        usuario, setpoint, description, tag, n_molino):

   sql = """
        insert INTO recom_implemented(
        val_actual,
        val_recom,
        created_at,
        updated_at,
        usuario,
        setpoint,
        description,
        tag,
        n_molino)
        VALUES ({0}, {1},'{2}','{3}','{4}',
                '{5}','{6}', {7} ,'{8}');
        """.format(
                val_actual,
                val_recom,
                created_at,
                updated_at,
                usuario,
                setpoint,
                description,
                tag,
                n_molino)
   exec_query(conn=output_conn, query=sql)


def ejecutar_implementadas_profit(recomendaciones, tiempo_respuesta, porcentaje_implementacion, output_conn,
                                  process_conn):
    for index, row in recomendaciones.iterrows():
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
        and t."timestamp" BETWEEN timestamp '{timestamp}'
        AND TIMESTAMP '{timestamp}' + INTERVAL '7 hours'
    order by
        t."timestamp" asc;
    """.format(timestamp=row['created_at'], tag=row['Tag'])

        df_process = make_dataframe(conn=process_conn, query=process_data_sql)
        df_process = df_process.dropna()

        # Fitramos los DataFrame vacios
        if df_process.empty:
            continue

        # Verificamos que la recomendacion fue respondida dentro
        # de las primeras 2 horas.
        if (row['updated_at'] - row['created_at']) > pd.Timedelta(tiempo_respuesta, 'h'):
            continue

        # Calculo del valor real utilizando los primero 15 minutos
        actual_value = df_process.head(15)['value'].mean().round(2)

        # Flags para determinar si la recomendación es de limite alto o bajo
        is_recom_hl = False
        is_recom_ll = False

        # Valor original de la recomendación
        recom_value = row['Recommended_Value']

        # Calculo del valor parcial a considerar
        recom_value_absoluto = abs(actual_value -
                                   recom_value) * (porcentaje_implementacion / 100)

        # Verificamos si la recomendacion es para el limite alto o bajo.
        # También se suma el valor absoluto con el recomendado para
        # aplicar logica en caso de que la recomendación sea
        # de implementación parcial.
        if actual_value <= recom_value:
            is_recom_hl = True
            recom_value_parcial = actual_value + recom_value_absoluto
        else:
            is_recom_ll = True
            recom_value_parcial = actual_value - recom_value_absoluto

        # Valor de limites en el momento de crearse la recomendación
        (timestamp_limit, ll_actual, hl_actual) = df_process.iloc[0, [0, 1, 2]]

        # Recomendación entre limites
        if hl_actual >= recom_value and ll_actual <= recom_value:
            insert_into_implementadas_profit(actual_value=actual_value,
                                             recom_value=recom_value,
                                             created_at=row['created_at'],
                                             updated_at=row['updated_at'],
                                             usuario=row['Usuario'],
                                             tag=row['Tag'],
                                             description=row['description'],
                                             hl_actual=hl_actual,
                                             ll_actual=ll_actual,
                                             entre_limites=True,
                                             n_molino=row['n_molino'],
                                             output_conn=output_conn)
            continue

        # Flag que indica que el valor original recomendado fue implementado
        # en su totalidad. (Recomendacion NO parcial).
        flag_total = False

        # Acotamos DataFrame de datos por minutos a dos horas
        df_process_2_horas = df_process[df_process['timestamp'] <= (
                df_process['timestamp'].iloc[0] + pd.Timedelta(hours=2))]

        for index, (timestamp, ll, hl, value) in df_process_2_horas.iterrows():
            # Momento en que el tag es contenido por la banda profit.
            if hl >= recom_value and ll <= recom_value:
                # Recortamos dataframe original desde el momento en que se
                # encontró que el tag es contenido por la banda profit.
                df_process = df_process.query('timestamp >= @timestamp')

                # Esta logica es utilizada para verificar que el operador
                # NO realizó cambios despues de haber implementado la
                # recomendación.

                # Volvemos a acotar dataframe. esta ves observando dos
                # horas desde que se cumplió condicion de la banda profit.
                df_process = df_process[df_process['timestamp'] <= (
                        df_process['timestamp'].iloc[0] + pd.Timedelta(hours=2))]

                # Promedios de los limites
                promedio_hl = df_process['hl'].mean()
                promedio_ll = df_process['ll'].mean()

                if is_recom_hl and hl == promedio_hl:
                    flag_total = True
                    insert_into_implementadas_profit(actual_value=actual_value,
                                                     recom_value=recom_value,
                                                     created_at=row['created_at'],
                                                     updated_at=row['updated_at'],
                                                     usuario=row['Usuario'],
                                                     tag=row['Tag'],
                                                     description=row['description'],
                                                     hl_actual=hl_actual,
                                                     ll_actual=ll_actual,
                                                     total=True,
                                                     n_molino=row['n_molino'],
                                                     output_conn=output_conn)
                    break

                if is_recom_ll and ll == promedio_ll:
                    flag_total = True
                    insert_into_implementadas_profit(actual_value=actual_value,
                                                     recom_value=recom_value,
                                                     created_at=row['created_at'],
                                                     updated_at=row['updated_at'],
                                                     usuario=row['Usuario'],
                                                     tag=row['Tag'],
                                                     description=row['description'],
                                                     hl_actual=hl_actual,
                                                     ll_actual=ll_actual,
                                                     total=True,
                                                     n_molino=row['n_molino'],
                                                     output_conn=output_conn)
                    break

        if flag_total is True:
            continue

        for index, (timestamp, ll, hl, value) in df_process_2_horas.iterrows():
            if hl >= recom_value_parcial and ll <= recom_value_parcial:
                insert_into_implementadas_profit(actual_value=actual_value,
                                                 recom_value=recom_value,
                                                 created_at=row['created_at'],
                                                 updated_at=row['updated_at'],
                                                 usuario=row['Usuario'],
                                                 tag=row['Tag'],
                                                 description=row['description'],
                                                 hl_actual=hl_actual,
                                                 ll_actual=ll_actual,
                                                 parcial=True,
                                                 n_molino=row['n_molino'],
                                                 output_conn=output_conn)
                break


def ejecutar_implementadas_no_profit(recomendaciones, tiempo_respuesta, porcentaje_implementacion, output_conn,
                                     process_conn):
    for index, row in recomendaciones.iterrows():
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
        and t."timestamp" BETWEEN timestamp '{timestamp}'
        AND TIMESTAMP '{timestamp}' + INTERVAL '7 hours'
    order by
        t."timestamp" asc;
    """.format(timestamp=row['created_at'], tag=row['Tag'])
        df_process = make_dataframe(conn=process_conn, query=process_data_sql)

        sube = False
        baja = False

        # Si el dataframe esta vacio, ir al sgte registro.
        if df_process.empty:
            continue

        # Verificamos que la recomendacion fue respondida dentro
        # de las primeras 2 horas.
        #import ipdb; ipdb.set_trace()
        if (row['updated_at'] - row['created_at']) > pd.Timedelta(tiempo_respuesta, 'h'):
            continue

        actual_value = df_process.head(15)['value'].mean().round(2)

        # Verificando si el movimiento del profit será acendente o
        # descendiente segun lo recomendado.
        if actual_value - row['Recommended_Value'] > 0:
            baja = True
        else:
            sube = True

        # Obtenemos el valor minimo al cual se esperaria llegar
        # con la recomendación
        recom_value = abs(actual_value - row['Recommended_Value']) * (porcentaje_implementacion / 100)
        valor_esperado = actual_value + recom_value

        # Recortamos los datos por minutos para obtener solamente las
        #  siguientes 2 horas
        # desde el momento en que se generaron recomendaciones.
        end_date = df_process.iloc[0, 0] + pd.Timedelta(hours=2)
        df_process = df_process[df_process['timestamp'] <= end_date]

        # Obtenemos el promedio del setpoint dentro de esas dos horas.
        value = df_process['value'].mean()

        # Iteramos en los datos por minutos
        for index, r in df_process.iterrows():

            # Si el movimiento esperado es ascendiente Y el valor
            # promediado es mayor o igual al valor
            # esperado, se considera implementada.
            if sube and value >= valor_esperado:
                insert_into_implementadas_no_profit(output_conn=output_conn,
                                                    val_actual=actual_value,
                                                    val_recom=row['Recommended_Value'],
                                                    created_at=row['created_at'],
                                                    updated_at=row['updated_at'],
                                                    usuario=row['Usuario'],
                                                    setpoint=row['Tag'],
                                                    description=row['description'],
                                                    tag=r['value'],
                                                    n_molino=row['n_molino'])
                break

                # Si el movimiento esperado es descendiente y el valor
                # promediado es menor o igual al valor
                # esperado, se considera implementada.
            if baja and value <= valor_esperado:
                insert_into_implementadas_no_profit(output_conn=output_conn,
                                                    val_actual=actual_value,
                                                    val_recom=row['Recommended_Value'],
                                                    created_at=row['created_at'],
                                                    updated_at=row['updated_at'],
                                                    usuario=row['Usuario'],
                                                    setpoint=row['Tag'],
                                                    description=row['description'],
                                                    tag=r['value'],
                                                    n_molino=row['n_molino'])
                break
