from recomendaciones import core
import psycopg2
import unittest
from datetime import datetime
import pandas as pd


class TestCore(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        host = "10.18.18.246"
        user = "postgres"
        password = "aapstgrs2020"
        dbname = "output_data"

        process_data_conn = psycopg2.connect(
            host=host, user=user, dbname="process_data", password=password)
        output_data_conn = psycopg2.connect(
            host=host, user=user, password=password, dbname=dbname)

        output_data_conn.autocommit = True
        cls.output_data_conn = output_data_conn
        cls.process_data_conn = process_data_conn

    def test_obtener_df_datos(self):

        datos_por_minutos = core.obtener_datos_por_minutos(process_conn=self.process_data_conn,
                                                           created_at="2020-08-20 06:00",
                                                           tag="SAG:FIC2188A",
                                                           hora_fin_turno="2020-08-20 14:00",
                                                           driving_factor="Profit")

        datos_por_minutos_ta = core.obtener_datos_por_minutos_turno_anterior(process_conn=self.process_data_conn,
                                                                             created_at="2020-08-20 06:00",
                                                                             tag="SAG:FIC2188A",
                                                                             driving_factor="Profit")

        self.assertTrue(not datos_por_minutos.empty, msg="DataFrame Datos por minutos vacio")
        self.assertTrue(not datos_por_minutos_ta.empty, msg="DataFrame Datos por minutos turno anterior vacio")


class TestImplementacion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        host = "10.18.18.246"
        user = "postgres"
        password = "aapstgrs2020"
        dbname = "output_data"

        process_data_conn = psycopg2.connect(
            host=host, user=user, dbname="process_data", password=password)
        output_data_conn = psycopg2.connect(
            host=host, user=user, password=password, dbname=dbname)

        output_data_conn.autocommit = True
        cls.output_data_conn = output_data_conn
        cls.process_data_conn = process_data_conn
        datos_por_minutos_hl = pd.read_csv('mock/datos_por_minutos_hl.csv')
        datos_por_minutos_ll = pd.read_csv('mock/datos_por_minutos_ll.csv')
        datos_por_minutos_hl['TimeStamp'] = pd.to_datetime(datos_por_minutos_hl['TimeStamp'])
        datos_por_minutos_ll['TimeStamp'] = pd.to_datetime(datos_por_minutos_ll['TimeStamp'])
        cls.datos_por_minutos_hl = datos_por_minutos_hl
        cls.datos_por_minutos_ll = datos_por_minutos_ll

    def test_respuesta_caducada(self):

        self.assertFalse(core.respuesta_caducada(created_at=datetime.strptime("2020-08-20 06:00", '%Y-%m-%d %H:%M'),
                                                 updated_at=datetime.strptime("2020-08-20 07:00", '%Y-%m-%d %H:%M'),
                                                 tiempo_respuesta=2), msg="No deberia estar caducado")

        self.assertTrue(core.respuesta_caducada(created_at=datetime.strptime("2020-08-20 06:00", '%Y-%m-%d %H:%M'),
                                                updated_at=datetime.strptime("2020-08-20 08:00", '%Y-%m-%d %H:%M'),
                                                tiempo_respuesta=2), msg="Deberia estar caducado")

        self.assertTrue(core.respuesta_caducada(created_at=datetime.strptime("2020-08-20 06:00", '%Y-%m-%d %H:%M'),
                                                updated_at=datetime.strptime("2020-08-20 08:10", '%Y-%m-%d %H:%M'),
                                                tiempo_respuesta=2), msg="Deberia estar caducado")

    def test_recom_hl_o_ll(self):
        self.assertIn('hl', core.recom_hl_o_ll(hl=500, ll=300, val_recom=600))
        self.assertIn('ll', core.recom_hl_o_ll(hl=500, ll=300, val_recom=200))
        self.assertIn('entre_limites', core.recom_hl_o_ll(hl=500, ll=300, val_recom=450))
        self.assertIn('entre_limites', core.recom_hl_o_ll(hl=500, ll=300, val_recom=500))
        self.assertIn('entre_limites', core.recom_hl_o_ll(hl=500, ll=300, val_recom=300))
        pd.merge

    def test_tipo_de_implementacion(self):
        self.assertIn('total', core.tipo_de_implementacion(df=self.datos_por_minutos_hl, val_recom=325, tipo_recom='hl'))
        self.assertIn('parcial', core.tipo_de_implementacion(df=self.datos_por_minutos_hl, val_recom=330, tipo_recom='hl'))
        self.assertIn('parcial', core.tipo_de_implementacion(df=self.datos_por_minutos_hl, val_recom=328, tipo_recom='hl'))
        self.assertIn('', core.tipo_de_implementacion(df=self.datos_por_minutos_hl, val_recom=340, tipo_recom='hl'))

        self.assertIn('total', core.tipo_de_implementacion(df=self.datos_por_minutos_ll, val_recom=260, tipo_recom='ll'))
        self.assertIn('parcial', core.tipo_de_implementacion(df=self.datos_por_minutos_ll, val_recom=240, tipo_recom='ll'))
        self.assertIn('parcial', core.tipo_de_implementacion(df=self.datos_por_minutos_ll, val_recom=245, tipo_recom='ll'))
        self.assertIn('', core.tipo_de_implementacion(df=self.datos_por_minutos_ll, val_recom=200, tipo_recom='ll'))

        self.assertIn('', core.tipo_de_implementacion(df=self.datos_por_minutos_hl, val_recom=221, tipo_recom='hl'))
        self.assertIn('', core.tipo_de_implementacion(df=self.datos_por_minutos_hl, val_recom=220, tipo_recom='hl'))

        self.assertIn('', core.tipo_de_implementacion(df=self.datos_por_minutos_ll, val_recom=280, tipo_recom='ll'))
        self.assertIn('', core.tipo_de_implementacion(df=self.datos_por_minutos_ll, val_recom=270, tipo_recom='ll'))

    def test_se_mantuvo_durante_x_horas(self):
        self.assertTrue(core.se_matuvo(df=self.datos_por_minutos_hl, val_recom=325, tipo_recom='hl', tipo_implementacion='total', valor_esperado=322))
        self.assertTrue(core.se_matuvo(df=self.datos_por_minutos_hl, val_recom=325, tipo_recom='hl', tipo_implementacion='parcial', valor_esperado=322))
        #self.assertTrue(core.se_matuvo(df=self.datos_por_minutos_hl))


if __name__ == '__main__':
    unittest.main()