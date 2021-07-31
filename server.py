import time, sys, cherrypy, os
from paste.translogger import TransLogger
from app import create_app
from pyspark.sql import SparkSession, Window
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from sedona.sql.types import GeometryType

def init_spark():
    spark = SparkSession \
        .builder \
        .master('spark://rodrigo-pc:7077') \
        .config("spark.serializer", KryoSerializer.getName) \
        .config("spark.kryo.registrator", SedonaKryoRegistrator.getName) \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    spark.sparkContext.addPyFile("engine.py")
    spark.sparkContext.addPyFile("app.py")
    spark.sparkContext.setCheckpointDir('checkpoints')
    #spark.sparkContext.addPyFile("tebd.zip")
    spark.sparkContext.setLogLevel('WARN')
    SedonaRegistrator.registerAll(spark)
    return spark
 
 
def run_server(app):
 
    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)
 
    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')
 
    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5432,
        'server.socket_host': 'localhost'
    })
 
    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()
 
 
if __name__ == "__main__":
    # Init spark context and load libraries
    spark = init_spark()
    originalDataPath = 'original_data'
    dbPath = 'data'
    app = create_app(spark, originalDataPath, dbPath)
 
    # start web server
    run_server(app)