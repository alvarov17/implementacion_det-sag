from .core import obtener_datos_por_minutos, obtener_datos_por_minutos_turno_anterior, get_df_resultado
import pandas as pd


def ejecutar_recomendaciones_implementadas_profit(process_data_conn, sql_achemy_conn, args, recom_profit):
    for index, row in recom_profit.iterrows():
        datos_por_minutos = obtener_datos_por_minutos(process_conn=process_data_conn, created_at=row['created_at'], tag=row['Tag'], driving_factor=row['Driving_Factor'])
        datos_por_minutos_turno_anterior = obtener_datos_por_minutos_turno_anterior(process_conn=process_data_conn, created_at=row['created_at'], tag=row['Tag'], driving_factor=row['Driving_Factor'])

        if datos_por_minutos.empty:
            continue

        if (row['updated_at'] - row['created_at']) > pd.Timedelta(args.tiempo_respuesta, 'h'):
            continue

        # Calculo del valor real utilizando los primero 15 minutos
        val_actual = datos_por_minutos_turno_anterior.head(15)['value'].mean().round(2)

        # Flags para determinar si la recomendación es de limite alto o bajo
        is_recom_hl = False
        is_recom_ll = False

        # Valor original de la recomendación
        val_recom = row['Recommended_Value']

        # Calculo del valor parcial a considerar
        recom_value_absoluto = abs(val_actual - val_recom) * (args.porcentaje_implementacion / 100)


        # Verificamos si la recomendacion es para el limite alto o bajo.
        # También se suma el valor absoluto con el recomendado para
        # aplicar logica en caso de que la recomendación sea
        # de implementación parcial.
        if val_actual <= val_recom:
            is_recom_hl = True
            recom_value_parcial = val_actual + recom_value_absoluto
        else:
            is_recom_ll = True
            recom_value_parcial = val_actual - recom_value_absoluto

        # Valor de limites en el momento de crearse la recomendación
        (timestamp_inicial, ll_inicial, hl_inicial) = datos_por_minutos.iloc[0, [0, 1, 2]]

        if hl_inicial >= val_recom and ll_inicial <= val_recom:
            resultado = get_df_resultado(val_actual=val_actual,
                                         val_recom=val_recom,
                                         created_at=row['created_at'],
                                         updated_at=row['updated_at'],
                                         tag_hl=hl_inicial,
                                         tag_ll=ll_inicial,
                                         setpoint=row['Tag'],
                                         usuario=row['Usuario'],
                                         description=row['description'],
                                         tag=pd.NA,
                                         parcial=pd.NA,
                                         total=pd.NA,
                                         entre_limites=True,
                                         n_molino=row['n_molino'])

            resultado.to_sql(name='recom_implemented', con=sql_achemy_conn, index=False, if_exists='append')
            continue

        datos_implementacion_total = datos_por_minutos.query('hl > @val_recom & ll < @val_recom')

        if not datos_implementacion_total.empty:
            datos_implementacion_total = datos_implementacion_total[datos_implementacion_total['timestamp'] <= (
                    datos_implementacion_total['timestamp'].iloc[0] + pd.Timedelta(hours=2))]

            (ll_seteado, hl_seteado) = datos_implementacion_total.iloc[0, [1, 2]]
            prom_hl = datos_implementacion_total['hl'].mean()
            prom_ll = datos_implementacion_total['ll'].mean()

            if is_recom_hl and prom_hl >= hl_seteado:
                resultado = get_df_resultado(val_actual=val_actual,
                                             val_recom=val_recom,
                                             created_at=row['created_at'],
                                             updated_at=row['updated_at'],
                                             tag_hl=hl_seteado,
                                             tag_ll=pd.NA,
                                             setpoint=row['Tag'],
                                             usuario=row['Usuario'],
                                             description=row['description'],
                                             tag=pd.NA,
                                             parcial=pd.NA,
                                             total=True,
                                             entre_limites=pd.NA,
                                             n_molino=row['n_molino'])

                resultado.to_sql(name='recom_implemented', con=sql_achemy_conn, index=False, if_exists='append')
                continue
            elif is_recom_ll and prom_ll <= ll_seteado:
                resultado = get_df_resultado(val_actual=val_actual,
                                             val_recom=val_recom,
                                             created_at=row['created_at'],
                                             updated_at=row['updated_at'],
                                             tag_hl=pd.NA,
                                             tag_ll=ll_seteado,
                                             setpoint=row['Tag'],
                                             usuario=row['Usuario'],
                                             description=row['description'],
                                             tag=pd.NA,
                                             parcial=pd.NA,
                                             total=True,
                                             entre_limites=pd.NA,
                                             n_molino=row['n_molino'])
                resultado.to_sql(name='recom_implemented', con=sql_achemy_conn, index=False, if_exists='append')
                continue

        if hl_inicial >= recom_value_parcial or ll_inicial <= recom_value_parcial:
            continue

        datos_implementacion_parcial = datos_por_minutos.query(
            'hl >= @recom_value_parcial & ll <= @recom_value_parcial')

        if not datos_implementacion_parcial.empty:
            continue


def ejecutar_recomendaciones_implementadas_setpoint(process_data_conn, sql_achemy_conn, args, recom_setpoint):
    for index, row in recom_setpoint.iterrows():
        datos_por_minutos = obtener_datos_por_minutos(process_conn=process_data_conn,
                                                      created_at=row['created_at'],
                                                      tag=row['Tag'],
                                                      driving_factor=row['Driving_Factor'])

        datos_por_minutos_turno_anterior = obtener_datos_por_minutos_turno_anterior(process_conn=process_data_conn,
                                                                                    created_at=row['created_at'],
                                                                                    tag=row['Tag'],
                                                                                    driving_factor=row['Driving_Factor'])
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
        if actual_value - row['Recommended_Value'] > 0:
            baja = True
        else:
            sube = True

        # Obtenemos el valor minimo al cual se esperaria llegar
        # con la recomendación
        recom_value = abs(actual_value - row['Recommended_Value']) * (args.porcentaje_implementacion / 100)

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
                                             val_recom=row['Recommended_Value'],
                                             created_at=row['created_at'],
                                             updated_at=row['updated_at'],
                                             tag_hl=pd.NA,
                                             tag_ll=pd.NA,
                                             setpoint=row['Tag'],
                                             usuario=row['Usuario'],
                                             description=row['description'],
                                             tag=max_tag_value,
                                             parcial=pd.NA,
                                             total=pd.NA,
                                             entre_limites=pd.NA,
                                             n_molino=row['n_molino'])

                resultado.to_sql(name='recom_implemented', con=sql_achemy_conn, index=False, if_exists='append')
                continue

        elif baja:
            datos_por_minutos = datos_por_minutos.query('value <= @valor_esperado')
            if not datos_por_minutos.empty:
                min_tag_value = datos_por_minutos['value'].min()
                resultado = get_df_resultado(val_actual=actual_value,
                                             val_recom=row['Recommended_Value'],
                                             created_at=row['created_at'],
                                             updated_at=row['updated_at'],
                                             tag_hl=pd.NA,
                                             tag_ll=pd.NA,
                                             setpoint=row['Tag'],
                                             usuario=row['Usuario'],
                                             description=row['description'],
                                             tag=min_tag_value,
                                             parcial=pd.NA,
                                             total=pd.NA,
                                             entre_limites=pd.NA,
                                             n_molino=row['n_molino'])
                resultado.to_sql(name='recom_implemented', con=sql_achemy_conn, index=False, if_exists='append')
                continue