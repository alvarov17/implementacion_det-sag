from .core import obtener_datos_por_minutos, obtener_datos_por_minutos_turno_anterior, get_df_resultado, \
    respuesta_caducada, recom_hl_o_ll, tipo_de_implementacion
import pandas as pd
import datetime


def ejecutar_recomendaciones_implementadas_profit(process_data_conn, sql_achemy_conn, args, recom_profit):
    for index, row in recom_profit.iterrows():
        hora_fin_turno = (row['created_at'] + datetime.timedelta(hours=8)).replace(minute=0, second=0, microsecond=0)

        datos_por_minutos = obtener_datos_por_minutos(process_conn=process_data_conn, created_at=row['created_at'], hora_fin_turno=hora_fin_turno, tag=row['tag'], driving_factor=row['driving_factor2'])
        datos_por_minutos_turno_anterior = obtener_datos_por_minutos_turno_anterior(process_conn=process_data_conn, created_at=row['created_at'], tag=row['tag'], driving_factor=row['driving_factor2'])

        if datos_por_minutos.empty:
            continue

        caducado = respuesta_caducada(created_at=row['created_at'], updated_at=row['updated_at'],
                                      tiempo_respuesta=args.tiempo_respuesta)

        if caducado:
            continue

        # Calculo del valor real utilizando los primero 15 minutos
        val_actual = datos_por_minutos_turno_anterior.head(15)['value'].mean().round(0)

        # Valor original de la recomendación
        val_recom = round(row['recommended_value'], 0)

        # Calculo del valor parcial a considerar
        recom_value_absoluto = abs(val_actual - val_recom) * (args.porcentaje_implementacion / 100)


        # Verificamos si la recomendacion es para el limite alto o bajo.
        # También se suma el valor absoluto con el recomendado para
        # aplicar logica en caso de que la recomendación sea
        # de implementación parcial.

        # Valor de limites en el momento de crearse la recomendación
        (timestamp_inicial, ll_inicial, hl_inicial) = datos_por_minutos.iloc[0, [0, 1, 2]]

        tipo_recom = recom_hl_o_ll(hl=hl_inicial, ll=ll_inicial, val_recom=val_recom)

        #recom_value_parcial = val_actual + recom_value_absoluto

        if tipo_recom == 'entre_limites':
            resultado = get_df_resultado(val_actual=val_actual,
                                         val_recom=val_recom,
                                         created_at=row['created_at'],
                                         updated_at=row['updated_at'],
                                         tag_hl=hl_inicial,
                                         tag_ll=ll_inicial,
                                         setpoint=row['tag'],
                                         usuario=row['user'],
                                         description=row['descripcion'],
                                         tag=pd.NA,
                                         parcial=pd.NA,
                                         total=pd.NA,
                                         entre_limites=True,
                                         modelo=row['modelo'])

            resultado.to_sql(name='recom_implemented', con=sql_achemy_conn, index=False, if_exists='append')
            continue

        tipo = tipo_de_implementacion(df=datos_por_minutos, val_recom=val_recom, tipo_recom=tipo_recom)

        if tipo == 'total' and tipo_recom == 'hl':
            (TimeStamp, ll, hl, value) = datos_por_minutos.query('hl >= @val_recom').iloc[0]
            resultado = get_df_resultado(val_actual=val_actual,
                                         val_recom=val_recom,
                                         created_at=row['created_at'],
                                         updated_at=row['updated_at'],
                                         tag_hl=hl,
                                         tag_ll=pd.NA,
                                         setpoint=row['tag'],
                                         usuario=row['user'],
                                         description=row['descripcion'],
                                         tag=pd.NA,
                                         parcial=pd.NA,
                                         total=True,
                                         entre_limites=pd.NA,
                                         modelo=row['modelo'])

            resultado.to_sql(name='recom_implemented', con=sql_achemy_conn, index=False, if_exists='append')

        elif tipo == 'total' and tipo_recom == 'll':
            (TimeStamp, ll, hl, value) = datos_por_minutos.query('ll <= @val_recom').iloc[0]
            resultado = get_df_resultado(val_actual=val_actual,
                                         val_recom=val_recom,
                                         created_at=row['created_at'],
                                         updated_at=row['updated_at'],
                                         tag_hl=pd.NA,
                                         tag_ll=ll,
                                         setpoint=row['tag'],
                                         usuario=row['user'],
                                         description=row['descripcion'],
                                         tag=pd.NA,
                                         parcial=pd.NA,
                                         total=True,
                                         entre_limites=pd.NA,
                                         modelo=row['modelo'])
            resultado.to_sql(name='recom_implemented', con=sql_achemy_conn, index=False, if_exists='append')
            continue
        if tipo == 'parcial' and tipo_recom == 'hl':
            resultado = get_df_resultado(val_actual=val_actual,
                                         val_recom=val_recom,
                                         created_at=row['created_at'],
                                         updated_at=row['updated_at'],
                                         tag_hl=pd.NA,
                                         tag_ll=pd.NA,
                                         setpoint=row['tag'],
                                         usuario=row['user'],
                                         description=row['descripcion'],
                                         tag=pd.NA,
                                         parcial=True,
                                         total=pd.NA,
                                         entre_limites=pd.NA,
                                         modelo=row['modelo'])
            resultado.to_sql(name='recom_implemented', con=sql_achemy_conn, index=False, if_exists='append')
            continue
        elif tipo == 'parcial' and tipo_recom == 'll':
            resultado = get_df_resultado(val_actual=val_actual,
                                         val_recom=val_recom,
                                         created_at=row['created_at'],
                                         updated_at=row['updated_at'],
                                         tag_hl=pd.NA,
                                         tag_ll=pd.NA,
                                         setpoint=row['tag'],
                                         usuario=row['user'],
                                         description=row['descripcion'],
                                         tag=pd.NA,
                                         parcial=True,
                                         total=pd.NA,
                                         entre_limites=pd.NA,
                                         modelo=row['modelo'])
            resultado.to_sql(name='recom_implemented', con=sql_achemy_conn, index=False, if_exists='append')
            continue
        elif tipo == '':
            continue
        continue

def ejecutar_recomendaciones_implementadas_setpoint(process_data_conn, sql_achemy_conn, args, recom_setpoint):
    for index, row in recom_setpoint.iterrows():
        hora_fin_turno = (row['created_at'] + datetime.timedelta(hours=8)).replace(minute=0, second=0, microsecond=0)

        datos_por_minutos = obtener_datos_por_minutos(process_conn=process_data_conn,
                                                      hora_fin_turno=hora_fin_turno,
                                                      created_at=row['created_at'],
                                                      tag=row['tag'],
                                                      driving_factor=row['driving_factor2'])

        datos_por_minutos_turno_anterior = obtener_datos_por_minutos_turno_anterior(process_conn=process_data_conn,
                                                                                    created_at=row['created_at'],
                                                                                    tag=row['tag'],
                                                                                    driving_factor=row['driving_factor2'])
        sube = False
        baja = False

        if datos_por_minutos.empty:
            continue

        # Verificamos que la recomendacion fue respondida dentro
        # de las primeras 2 horas.
        if (row['updated_at'] - row['created_at']) > pd.Timedelta(args.tiempo_respuesta, 'h'):
            continue

        actual_value = datos_por_minutos_turno_anterior.head(15)['value'].mean().round(2)

        # Verificando si el movimiento del profit será acendente o
        # descendiente segun lo recomendado.
        if actual_value - row['recommended_value'] > 0:
            baja = True
        else:
            sube = True

        # Obtenemos el valor minimo al cual se esperaria llegar
        # con la recomendación
        recom_value = abs(actual_value - row['recommended_value']) * (args.porcentaje_implementacion / 100)

        if baja:
            valor_esperado = actual_value - recom_value
        elif sube:
            valor_esperado = actual_value + recom_value

        # Obtenemos el promedio del setpoint dentro de esas dos horas.
        value = datos_por_minutos['value'].mean()

        if sube:
            datos_por_minutos = datos_por_minutos.query('value >= @valor_esperado')
            if not datos_por_minutos.empty:
                max_tag_value = datos_por_minutos['value'].max()
                resultado = get_df_resultado(val_actual=actual_value,
                                             val_recom=row['recommended_value'],
                                             created_at=row['created_at'],
                                             updated_at=row['updated_at'],
                                             tag_hl=pd.NA,
                                             tag_ll=pd.NA,
                                             setpoint=row['tag'],
                                             usuario=row['user'],
                                             description=row['descripcion'],
                                             tag=max_tag_value,
                                             parcial=pd.NA,
                                             total=pd.NA,
                                             entre_limites=pd.NA,
                                             modelo=row['modelo'])

                resultado.to_sql(name='recom_implemented', con=sql_achemy_conn, index=False, if_exists='append')
                continue

        elif baja:
            datos_por_minutos = datos_por_minutos.query('value <= @valor_esperado')
            if not datos_por_minutos.empty:
                min_tag_value = datos_por_minutos['value'].min()
                resultado = get_df_resultado(val_actual=actual_value,
                                             val_recom=row['recommended_value'],
                                             created_at=row['created_at'],
                                             updated_at=row['updated_at'],
                                             tag_hl=pd.NA,
                                             tag_ll=pd.NA,
                                             setpoint=row['tag'],
                                             usuario=row['user'],
                                             description=row['descripcion'],
                                             tag=min_tag_value,
                                             parcial=pd.NA,
                                             total=pd.NA,
                                             entre_limites=pd.NA,
                                             modelo=row['modelo'])
                resultado.to_sql(name='recom_implemented', con=sql_achemy_conn, index=False, if_exists='append')
                continue