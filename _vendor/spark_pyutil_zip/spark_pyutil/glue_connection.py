# -*- coding: utf-8 -*-
from spark_pyutil.utility import exists_files, get_and_validate_json, is_dir, delete_path
import logging
import boto3
import re
import sharepy
import pandas as pd
import io
from pandas import ExcelFile

_logger = logging.getLogger(__name__) 

class GlueJDBCConectionError(Exception): 
    pass

class GlueJDBCConection:
    
    drivers_type = {
        "oracle": "oracle.jdbc.driver.OracleDriver",
        "postgresql": "org.postgresql.Driver",
        "sap": "com.sap.db.jdbc.Driver",
        "sqlserver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    
    def __init__(self, spark_session, conneciton_parameter, region):
        
        _logger.info("############### ENTER IN __init__ #################")
        self.__spark_session = spark_session
        self.__conneciton_parameter=conneciton_parameter
        _logger.info("Open Sessions")
        session = boto3.session.Session()
        self.__glue_client = session.client('glue',region)
        # _logger.info("renan")
        _logger.info(region)

    def __get_connection(self):
        _logger.info("############### ENTER IN __get_connection #################")
        name = self.__conneciton_parameter['GlueConnection']
        _logger.info("GlueConnection:"+name)
        connection = self.__glue_client.get_connection(Name= name)
        
        if str(connection['Connection']['ConnectionType']).upper() not in  ('JDBC','MONGODB'):
            raise GlueJDBCConectionError("Connection Type on glue is not JDBC or MONGODB")
        # _logger.info("############### OUT OF __get_connection #################")
        return connection
    
    def __get_connection_type(self,url):
        _logger.info("############### ENTER IN __get_connection_type #################")
        if 'mongodb' in url:
            _logger.info("############### ENTER IN if 'mongodb' in url #################")
            return 'mongo'
        elif 'sharepoint' in url:
            _logger.info("############### ENTER IN if 'sharepoint' in url #################")
            return 'sharepoint'
        else:
            _logger.info("############### ENTER IN else #################")
            return re.match(r'^jdbc:([a-zA-Z]*)(:.*)',url).group(1)
    
    def __driver_type(self,url):
        _logger.info("############### ENTER IN __driver_type #################")
        if 'mongodb' in url: 
            return 'mongo'
        elif 'sharepoint' in url: 
            return 'sharepoint'
        else:
            dbtype=self.__get_connection_type(url)
            try:
                return GlueJDBCConection.drivers_type[dbtype]
            except:
                raise GlueJDBCConectionError('This database is not enable on Spark-pyutil: {}',dbtype)
        
    def read_from_odbc(self):
        _logger.info("############### ENTER IN read_from_odbc #################")
        connection = self.__get_connection()
        _logger.info("############### BACK TO read_from_odbc #################")
        formatCon = connection['Connection']['ConnectionType']
        # _logger.info(f"############### formatCon = {formatCon} #################")
        
        if formatCon == 'MONGODB':
            url = connection['Connection']['ConnectionProperties']['CONNECTION_URL'] #quebrar para montar a string
        else:
            url = connection['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL'] 
            
        user= connection['Connection']['ConnectionProperties']['USERNAME']
        password= connection['Connection']['ConnectionProperties']['PASSWORD']
        _logger.info(f"USERNAME={user} PASSWORD={password}")
        
        driver = self.__driver_type(url)
        # glue connection put two "//" and does not work on Glue Job ¬¬
        if self.__get_connection_type(url) == 'oracle':
            url = url.replace("//@","@")
        
        if formatCon == 'MONGODB':
            _logger.info("############### ENTER IN if formatCon == 'MONGODB' #################")
            url_mongodb = url
            _logger.info(f"############### URL = {url_mongodb} #################")
            ip_and_port_mongo_partial = url_mongodb.replace('mongodb://','')
            ip_and_port_mongo = ip_and_port_mongo_partial.split('/')[0]
            
            uri_mongo = "mongodb://" + user + ":" + password + "@" + ip_and_port_mongo + '/' + self.__conneciton_parameter['dbtable'] + '?ssl=true'
            _logger.info(f"############### URI = {uri_mongo} #################")
            read_stm = self.__spark_session.read.format("mongo").option("uri", uri_mongo)
            
        elif formatCon == 'JDBC' and url.find('sharepoint') != -1:
            _logger.info("############### ENTER IN if formatCon == 'JDBC' and is SHAREPOINT#################")
            url_sharepoint = url
            _logger.info(f"############### URL = {url_sharepoint} #################")
            server_partial = url.split("//")[1]
            
            SERVER = server_partial.split(":")[0]
            USER = user
            PASSWORD = password
            _logger.info(f"############### ReadOptions = {self.__conneciton_parameter['ReadOptions']} #################")
            file_url = self.__conneciton_parameter['ReadOptions'][0]['OptionValue']
            file_name = self.__conneciton_parameter['ReadOptions'][1]['OptionValue']
            
            sharepoint = sharepy.connect(SERVER, USER, PASSWORD)
            # sharepoint = sharepy.connect(SERVER)
            #  Salva na mesma pasta que o script está sendo utilizado
            
            # destination=s3fs+'/'+file_name

            r = sharepoint.getfile(file_url, filename=file_name)
            df_pandas = pd.read_excel(file_name, index_col=None, header=1)  
            
            count_row = df_pandas.shape[0]  # Gives number of rows
            count_col = df_pandas.shape[1]  # Gives number of columns
            
            _logger.info(f"############### df_pandas rows count = {count_row} #################")
            _logger.info(f"############### df_pandas columns count = {count_col} #################")
            
            columns_with_null = df_pandas.columns[df_pandas.isnull().any()].tolist()
            _logger.info(f"############### Columns with null values before : = {columns_with_null} #################")
            
            
            columns_list = df_pandas.columns[df_pandas.any()].tolist()
            
            df_pandas = df_pandas.astype(str)
            
            
            # #removing null values with -1
            # for column in columns_with_null:
            #   if df_pandas[column].dtypes == 'object':
            #       df_pandas[column] = df_pandas[column].fillna("-1")
            #   else:
            #       df_pandas[column] = df_pandas[column].fillna(-1)
            
            # columns_with_null = df_pandas.columns[df_pandas.isnull().any()].tolist()
            _logger.info(f"############### Columns with null values after : = {columns_with_null} #################")
            df_pandas.columns = df_pandas.columns.str.lower()
            df_pandas.columns = df_pandas.columns.str.replace(' ','_')
            df_pandas.columns = df_pandas.columns.str.replace('.','')
            df_pandas.columns = df_pandas.columns.str.replace('/','')
            df_pandas.columns = df_pandas.columns.str.replace('(','')
            df_pandas.columns = df_pandas.columns.str.replace(')','')
            df_pandas.columns = df_pandas.columns.str.replace('ç','c')
            df_pandas.columns = df_pandas.columns.str.replace('ã','a')
            df_pandas.columns = df_pandas.columns.str.replace('á','a')
            df_pandas.columns = df_pandas.columns.str.replace('à','a')
            df_pandas.columns = df_pandas.columns.str.replace('â','a')
            df_pandas.columns = df_pandas.columns.str.replace('é','e')
            df_pandas.columns = df_pandas.columns.str.replace('ê','e')
            df_pandas.columns = df_pandas.columns.str.replace('è','e')
            df_pandas.columns = df_pandas.columns.str.replace('í','i')
            df_pandas.columns = df_pandas.columns.str.replace('î','i')
            df_pandas.columns = df_pandas.columns.str.replace('ì','i')
            df_pandas.columns = df_pandas.columns.str.replace('ó','o')
            df_pandas.columns = df_pandas.columns.str.replace('õ','o')
            df_pandas.columns = df_pandas.columns.str.replace('ô','o')
            df_pandas.columns = df_pandas.columns.str.replace('ò','o')
            df_pandas.columns = df_pandas.columns.str.replace('ú','u')
            df_pandas.columns = df_pandas.columns.str.replace('ù','u')
            df_pandas.columns = df_pandas.columns.str.replace('û','u')
            
            
            
            df_spark = self.__spark_session.createDataFrame(df_pandas)
            
            # #VERIFICAR PARA NAO GERAR ARQUIVO
            # s = sharepy.connect(SERVER, username=USER, password=PASSWORD)
            # r = s.get(file_url)
            # f = io.BytesIO(r.content)

            
            # excel_data = ExcelFile(f)
            # df = excel_data.parse(excel_data.sheet_names[-1])
            
            # # df = pd.read_excel(f)
            # df_spark = self.__spark_session.createDataFrame(df)
            
            _logger.info(f"############### URI = {url_sharepoint} #################")
            
        else:
            read_stm = self.__spark_session.read.format(formatCon) \
                        .option("url",url) \
                        .option("user", user) \
                        .option("password", password) \
                        .option("driver", driver)
                        
        if self.__conneciton_parameter['ReadFormat'] not in ('mongo', 'sharepoint'):
            if 'dbtable' in self.__conneciton_parameter and 'query' in self.__conneciton_parameter:
                raise GlueJDBCConectionError("For use JDBC connection you must set only one option, dbtable or query")
            elif 'dbtable' in self.__conneciton_parameter:
                read_stm = read_stm.option("dbtable", self.__conneciton_parameter['dbtable'])
            elif 'query' in self.__conneciton_parameter:  
                read_stm = read_stm.option("query", self.__conneciton_parameter['query'])
            else:
                raise GlueJDBCConectionError("'dbtable' or 'query' is a required property")


        if self.__conneciton_parameter['ReadFormat'] in ('sharepoint'):
            df = df_spark
        else:
            df = read_stm.load()
                        
        return df