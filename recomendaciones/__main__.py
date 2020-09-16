# File name: __main__.py
# Description: Archivo ejecutable para obtener implementación
# de recomendaciones
# Author: Alvaro Valenzuela
# Date: 10-09-2020

import sys
import argparse
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta

from .core import eliminar_registros, obtener_recom_profit, obtener_recom_setpoint, ejecutar_implementadas_profit, ejecutar_implementadas_no_profit

parser = argparse.ArgumentParser(
    description="Obtiene recomendaciones implementadas")
parser.add_argument('-env', '--environment',
                    type=str,
                    required=True,
                    metavar="ambiente",
                    help="Escritura en base de datos de desarrollo o en producción?",
                    choices=['dev', 'prod'])

parser.add_argument('-fi', '--fecha_inicio',
                    type=str,
                    required=False,
                    metavar="Fecha de inicio",
                    help="Fecha de inicio del análisis",
                    default=(datetime.now() + timedelta(hours=-2 * 24)
                             ).strftime("%Y-%m-%d %H:%M"))

parser.add_argument('-ft', '--fecha_termino',
                    type=str,
                    required=False,
                    metavar="Fecha de termino",
                    help="Fecha de termino del analisis",
                    default=(datetime.now() + timedelta(hours=-3)
                             ).strftime("%Y-%m-%d %H:%M"))

parser.add_argument('-tr', '--tiempo_respuesta',
                    type=int,
                    required=False,
                    help="Tiempo de respuesta máxima permitida para el operador",
                    default=2)

parser.add_argument('-pi', '--porcentaje_implementacion',
                    type=int,
                    required=False,
                    default=50)

args = parser.parse_args()


def main():
    host = "10.18.18.248"
    user = "aapmcdet"
    password = "codelco.2020"
    dbname = "det_pmc_process-data_dev"
    if args.environment == 'prod':
        host = "10.18.18.247"
        user = "aasag_dch"
        password = "SAGChuqui2020"
        dbname = "det_pmc_output-data_prod"

    process_data_conn = psycopg2.connect(
        """dbname=det_pmc_process-data_prod
        host=10.18.18.247 user=aasag_dch password=SAGChuqui2020""")

    output_data_conn = psycopg2.connect(
        host=host, user=user, password=password, dbname=dbname)

    output_data_conn.autocommit = True

    eliminar_registros(fi=args.fecha_inicio, ff=args.fecha_termino, conn=output_data_conn)

    recom_profit = obtener_recom_profit(fi=args.fecha_inicio, ff=args.fecha_termino, conn=output_data_conn)
    recom_setpoint = obtener_recom_setpoint(fi=args.fecha_inicio, ff=args.fecha_termino, conn=output_data_conn)


    ejecutar_implementadas_profit(tiempo_respuesta=args.tiempo_respuesta,
                                  porcentaje_implementacion=args.porcentaje_implementacion,
                                  recomendaciones=recom_profit,
                                  output_conn=output_data_conn,
                                  process_conn=process_data_conn)
    ejecutar_implementadas_no_profit(recomendaciones=recom_setpoint,
                                     tiempo_respuesta=args.tiempo_respuesta,
                                     porcentaje_implementacion=args.porcentaje_implementacion,
                                     output_conn=output_data_conn,
                                     process_conn=process_data_conn)

    exit()



if __name__ == '__main__':
    main()
