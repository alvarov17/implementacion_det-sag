# File name: __main__.py
# Description: Archivo ejecutable para obtener implementación
# de recomendaciones
# Author: Alvaro Valenzuela
# Date: 10-09-2020

import argparse
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
from sqlalchemy import create_engine

from .core import eliminar_registros, obtener_recom_profit, obtener_recom_setpoint
from .recomendaciones import ejecutar_recomendaciones_implementadas_profit, ejecutar_recomendaciones_implementadas_setpoint

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
    dbname = "det_pmc_output-data_dev"
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

    sql_achemy_conn = create_engine(f"postgresql://{user}:{password}@{host}/{dbname}")

    eliminar_registros(fi=args.fecha_inicio, ff=args.fecha_termino, conn=output_data_conn)

    recom_profit = obtener_recom_profit(fi=args.fecha_inicio, ff=args.fecha_termino, conn=output_data_conn)
    recom_setpoint = obtener_recom_setpoint(fi=args.fecha_inicio, ff=args.fecha_termino, conn=output_data_conn)

    ejecutar_recomendaciones_implementadas_profit(process_data_conn=process_data_conn,
                                                  sql_achemy_conn=sql_achemy_conn,
                                                  args=args,
                                                  recom_profit=recom_profit)

    ejecutar_recomendaciones_implementadas_setpoint(process_data_conn=process_data_conn,
                                                    sql_achemy_conn= sql_achemy_conn,
                                                    args=args,
                                                    recom_setpoint=recom_setpoint)
    exit()


if __name__ == '__main__':
    main()
