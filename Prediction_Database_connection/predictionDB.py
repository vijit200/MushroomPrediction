import os
from os import  listdir
from logger import  logging
import pandas as pd
import database_connect as connection
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

class DBOperationPre:
    
    def __init__(self):

        self.badFilePath = "Prediction_Raw_files_validated/Bad_Raw"
        self.goodFilePath = "Prediction_Raw_files_validated/Good_Raw/"
        self.logger = logging

    def Preloadtodatabase(self):

        self.logger.info("{} MAKING CONNECTION TO DATABASE {}".format('='*20,'='*20))
        try:

            zip_path = r'E:\secure-connect-mushrooms.zip'
            client_id = 'caOGgwstrqfMbPBIEjpSYBmn'
            client_secret = '2wPdzqu,wUISxwUmqYXgspRDm..8j-lQL1eZLjCpnsbpZdm9zTKkmMo5i4r8fQeUJ4YHI_+Hzzs.j0DbCe+.gWEPZuqA-M+Z4BcTr7NP9Q_7ZTekpCSZ2-jUxB.2lhC_'
            keyspace = 'mush'
            table_name = 'mushrooms'

            cassandra = connection.cassandra_operations(zip_path,
                                                client_id,
                                                client_secret,
                                                keyspace,
                                            table_name)

            self.logger.info('{} CONNECTION DONE SUCCESSFULLY {}'.format('='*20,'='*20))

            self.logger.info('{} NOW INSERTING DATA INTO TABLE {}'.format('='*20,'='*20))

            cloud_config= {'secure_connect_bundle': 'E:\secure-connect-mushrooms.zip'}
            auth_provider = PlainTextAuthProvider('caOGgwstrqfMbPBIEjpSYBmn','2wPdzqu,wUISxwUmqYXgspRDm..8j-lQL1eZLjCpnsbpZdm9zTKkmMo5i4r8fQeUJ4YHI_+Hzzs.j0DbCe+.gWEPZuqA-M+Z4BcTr7NP9Q_7ZTekpCSZ2-jUxB.2lhC_')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()
            row = session.execute("select release_version from system.local").one()
            row=session.execute("select * from system_schema.keyspaces")

            session = cluster.connect('mush')

            session.default_timeout = 60

            

            session.execute('DROP TABLE IF EXISTS mush.mushrooms_Predict;')
            self.logger.info('table dropped!!....')

            

            df = pd.read_csv(self.goodFilePath + 'mushroomMain_02034068_000001.csv')

            self.logger.info('Reading csv file from good folder done')


            l = list(df.columns)

            l1 = []
            for i in l:
                if '-' in i:
                    l1.append(i.replace('-','_'))
                else:
                    l1.append(i)

            df.columns = l1


            id = []
            for i in range(len(df)):
                id.append(i)

            df['id'] = id

            self.logger.info('id inserted into data')

            df = df[['id', 'cap_shape', 'cap_surface', 'cap_color', 'bruises', 'odor',
                'gill_attachment', 'gill_spacing', 'gill_size', 'gill_color',
                'stalk_shape', 'stalk_root', 'stalk_surface_above_ring',
                'stalk_surface_below_ring', 'stalk_color_above_ring',
                'stalk_color_below_ring', 'veil_type', 'veil_color', 'ring_number',
                'ring_type', 'spore_print_color', 'population', 'habitat']]

                

            cassandra.bulk_upload(data = df,table_name='mushrooms_Predict',create_new_table=True)

            cassandra.session.default_timeout = 30

            self.logger.info('Table created successfully and insertion done to database')



        except OSError as s:

            self.logger.info('OSError occured in database is : ' + str(s))

            raise s

        except Exception as e:

            self.logger.info('Error occured while creating database is : ' + str(e))

            raise e

    def selectingfromdatabase(self):

        try:

            zip_path = r'E:\secure-connect-mushrooms.zip'
            client_id = 'caOGgwstrqfMbPBIEjpSYBmn'
            client_secret = '2wPdzqu,wUISxwUmqYXgspRDm..8j-lQL1eZLjCpnsbpZdm9zTKkmMo5i4r8fQeUJ4YHI_+Hzzs.j0DbCe+.gWEPZuqA-M+Z4BcTr7NP9Q_7ZTekpCSZ2-jUxB.2lhC_'
            keyspace = 'mush'
            table_name = 'mushrooms'

            cassandra = connection.cassandra_operations(zip_path,
                                                client_id,
                                                client_secret,
                                                keyspace,
                                            table_name)

            cassandra.session.default_timeout = 30


            self.df = cassandra.read_data(table_name = 'mushrooms_Predict')

            self.df.drop('id',axis=1,inplace=True)

            path = 'Prediction_database/'

            if not os.path.isdir(path):
                os.makedirs(path)

            self.df.to_csv('Prediction_database/InputFile.csv',index=False)

            self.logger.info('Input file created')

        except Exception as e:

            self.logger.info('Error occured during creating input csv file is : ' + str(e))

            raise e
