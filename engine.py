import json
import requests
import os
import geopandas as gpd
import datetime
import numpy as np
import h3
import shutil
import logging
import googlemaps

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType, ArrayType, DateType, TimestampType
import pyspark.sql.functions as F

from sedona.sql.types import GeometryType
from shapely.geometry import Point

from pygenetic import ChromosomeFactory, GAEngine, Utils

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)
APIKEY = 'AIzaSyCXq8CMRpAVrN_GRcqVXYn8Jqj-wJ6_x9g'
gps = googlemaps.Client(APIKEY)

def point2tuple(point):
    return point.y, point.x

def checkDistance(riders, drivers):
    riders = [point2tuple(i) for i in riders]
    drivers = [point2tuple(i) for i in drivers]
    result = [[gps.distance_matrix(i, j)["rows"][0]["elements"][0]["duration"]["value"]] for i, j in zip(riders, drivers)]
    return result

h3Udf = F.udf(lambda x: h3.h3_to_geo(x), ArrayType(FloatType()))

getLat = F.udf(lambda x: x.y, FloatType())
getLon = F.udf(lambda x: x.x, FloatType())

def getDriverPosition(x):
    url = "http://localhost:5432/driver/{}/location".format(x)
    result = requests.get(url).json()
    try:
        return [float(result['lon']), float(result['lat'])]
    except:
        return None

getDriverPositionUdf = F.udf(getDriverPosition, ArrayType(FloatType()))

class AllocationEngine:
    """A movie recommendation engine
    """

    def db(self, *text):
        return os.path.join(self.dbPath, *text)
    
    def riderRequest(self, data):
        table = self.spark.createDataFrame([[data["riderid"], data["lat"], data["lon"], data["requestTime"]]], self.ridersRequestSchema)
        table.toPandas().to_parquet(self.db('riders_request', '{}-{}.parquet'.format(data['requestTime'].strftime("%Y%m%d-%H%M%S-%f"), data['riderid'])))

    def driverUpdate(self, data):
        table = self.spark.createDataFrame([[data["driverid"], data["requestTime"], data["active"]]], self.driversStatusSchema)
        table.toPandas().to_parquet(self.db('drivers_status', '{}-{}.parquet'.format(data['requestTime'].strftime("%Y%m%d-%H%M%S-%f"), data['driverid'])))
    
    def preprocessing(self):
        try:
            shutil.rmtree(self.dbPath)
        except:
            pass
        
        try:
            shutil.rmtree('spark-warehouse')
        except:
            pass

        distancesSchema = StructType([
            StructField("sourceid", IntegerType(), True),
            StructField("destid", IntegerType(), True),
            StructField("hod", IntegerType(), True),
            StructField("mean_travel_time", FloatType(), True),
            StructField("standard_deviation_travel_time", FloatType(), True),
            StructField("geometric_mean_travel_time", FloatType(), True),
            StructField("geometric_standard_deviation_travel_time", FloatType(), True),
        ])

        distances = self.spark.read.csv(os.path.join(self.originalDataPath, 'washington_DC-censustracts-2019-1-All-HourlyAggregate.csv'), 
                                schema=distancesSchema,
                                header=True)

        distances.write.parquet(self.db('base/distances.parquet'))

        rawMobileUseSchema = StructType([
            StructField("hexid", StringType(), True),
            StructField("dayType", StringType(), True),
            StructField("traversals", IntegerType(), True),
            StructField("wktGeometry", StringType(), True),
        ])

        rawMobileUse = self.spark.read.csv(os.path.join(self.originalDataPath, 'washington_DC-traversals.csv'), schema=rawMobileUseSchema, header=True) \
            .withColumn('point', h3Udf(F.col('hexid'))) \
            .withColumn('lat', (lambda x: x[0])(F.col('point')).cast('double')) \
            .withColumn('lon', (lambda x: x[1])(F.col('point')).cast('double')) \
            .drop('point').drop('wktGeometry')

        rawMobileUse.write.parquet(self.db('base/rawMobileUse.parquet'))

        with open(os.path.join(self.originalDataPath, 'washington_DC_censustracts.json')) as f:
            zonesGeoPandas = gpd.GeoDataFrame.from_features(
                json.loads("".join(f.readlines()))
            )

        zonesGeoPandas['MOVEMENT_ID'] = zonesGeoPandas['MOVEMENT_ID'].astype(int)
        zonesGeoPandas['geometry'] = zonesGeoPandas['geometry'].astype(str)
        zonesGeoPandas = zonesGeoPandas[['MOVEMENT_ID', 'DISPLAY_NAME', 'geometry']]
        zonesGeoPandas.to_parquet(self.db('base/rawZones.parquet'), index=False)

        taxi = self.spark.read.format('csv') \
            .option("header", "true") \
            .option("delimiter", "|") \
            .option("inferSchema","true") \
            .load(os.path.join(self.originalDataPath, 'taxi_2019/taxi_2019_01.txt')) \
            .filter('ORIGIN_BLOCK_LATITUDE is not null')

        taxi.select(
            taxi['ORIGIN_BLOCK_LATITUDE'].alias('lat'), 
            taxi['ORIGIN_BLOCK_LONGITUDE'].alias('lon'), 
            F.to_timestamp(taxi['ORIGINDATETIME_TR'], 'MM/dd/yyyy HH:mm').alias('date')
        ).write.parquet(self.db('base/taxi.parquet'))

    def solveOptProblem(self, evolutions):
        df = self.filledDistances.groupBy('riderid').pivot('driverid').sum('actualTime').toPandas()
        print(df)
        M = df.values

        def fitness(x):
            return sum(M[(tuple(range(self.nRiders)), tuple(x))])

        factory = ChromosomeFactory.ChromosomeRangeFactory(
            minValue=1,
            maxValue=self.nDrivers,
            noOfGenes=self.nRiders,
            duplicates=False
        )

        # não ta paralelo :<
        ga = GAEngine.GAEngine(
            factory = factory,
            population_size = 1000,
            fitness_type = 'min',
            cross_prob = 0.9,
            mut_prob = 0.1
        )

        ga.addCrossoverHandler(Utils.CrossoverHandlers.OX, 3)
        ga.addCrossoverHandler(Utils.CrossoverHandlers.PMX, 3)
        ga.addMutationHandler(Utils.MutationHandlers.swap, 2)
        ga.setSelectionHandler(Utils.SelectionHandlers.best)
        ga.setFitnessHandler(fitness)
        
        ga.evolve(evolutions)
        
        self.allocationList = np.array([M[:, 0], df.columns[ga.hall_of_fame[0]]]).T.astype(int).tolist()
        self.allocation = self.spark.createDataFrame(self.allocationList, ['riderid', 'driverid'])

    def updateDrivers(self):
        ### change to return dict-like
        self.activeDrivers = self.activeDrivers.groupBy('driverid') \
            .agg(F.max('requestTime').alias('requestTime')) \
            .join(self.activeDrivers, ['driverid', 'requestTime']) \
            .filter(F.col('active') == True)
        
        currentDriversList = self.activeDrivers \
            .select('driverid') \
            .collect()
        
        self.currentDriversListCoord = [[i[0]] + getDriverPosition(i[0]) for i in currentDriversList]
        self.currentDriversList = [[i[0], Point(i[1], i[2])] for i in self.currentDriversListCoord]

        self.currentDriversSchema = StructType([
            StructField("driverid", IntegerType(), True),
            StructField("point", GeometryType(), True)
        ])
        
        self.currentDrivers = self.spark.createDataFrame(self.currentDriversList, self.currentDriversSchema)
        self.currentDrivers.createOrReplaceTempView('currentDrivers')

    def processBatch(self, df, id, evolutions=50):
        now = datetime.datetime.utcnow()
        self.lastUpdate = now
        """ self.currentRiders = self.currentRiders.filter(F.col('requestTime') >= self.lastUpdate) """
        self.currentRiders = []
        self.updateDrivers()

        sessionDf = self.spark.createDataFrame(df.collect(), schema=self.ridersRequestSchema) \
            .withColumn('point', F.udf(lambda x, y: Point(x, y), GeometryType())(F.col('lon'), F.col('lat')))
        sessionDf.createOrReplaceTempView('currentRiders')
        sessionDf.show()

        currentDriversPos = self.spark.sql('select t.driverid, t.point, s.movement_id from currentDrivers t join zones s on st_intersects(t.point, s.geometry) = true')
        currentDriversPos.createOrReplaceTempView('currentDriversPos')

        currentRidersPos = self.spark.sql('select t.riderid, t.point, t.requestTime time, s.movement_id from currentRiders t join zones s on st_intersects(t.point, s.geometry) = true')
        currentRidersPos.createOrReplaceTempView('currentRidersPos')

        self.nDrivers = currentDriversPos.count()
        self.nRiders = currentRidersPos.count()

        ridersDrivers = self.spark.sql('select t.time requestTime, t.riderid, t.point as riderLocation, t.movement_id as destid, s.driverid, s.point as driverLocation, s.movement_id as sourceid from currentRidersPos t, currentDriversPos s')

        hod = now.hour

        filteredDistances = self.distances.filter(F.col('hod') == hod).select(
            "sourceid", 
            "destid", 
            F.col("mean_travel_time").alias('avg'), 
            F.col("standard_deviation_travel_time").alias('std')
        )

        joinedDistances = ridersDrivers.join(
            filteredDistances, 
            ['sourceid', 'destid'], 
            'left'
        ).withColumn('avg', F.when(F.col('destid') == F.col('sourceid'), 0).otherwise(F.col('avg'))) \
        .withColumn('std', F.when(F.col('destid') == F.col('sourceid'), 300).otherwise(F.col('std'))) \
        .fillna(99999)

        window = Window.partitionBy(joinedDistances['riderid']).orderBy(joinedDistances['avg'])

        selectedDistances = joinedDistances \
            .select('*', F.rank().over(window).alias('rank')) \
            .filter(F.col('rank') <= 2 * self.nRiders)
        
        gpsDistances = checkDistance(*(selectedDistances.select('riderLocation', 'driverLocation').toPandas().values.T))
        gpsDistancesDf = self.spark.createDataFrame(gpsDistances, ['actualTime']) \
            .withColumn('rId', F.monotonically_increasing_id())
        actualDistances = selectedDistances \
            .withColumn('rId', F.monotonically_increasing_id()) \
            .join(gpsDistancesDf, ['rId']) \
            .select('riderid', 'driverid', 'actualTime')

        self.filledDistances = joinedDistances.join(
            actualDistances,
            ['riderid', 'driverid'],
            'left'
        ).withColumn('actualTime', F.coalesce(F.col('actualTime'), F.col('avg') + 3 * F.col('std')))

        self.solveOptProblem(evolutions)

        self.lastAllocation = ridersDrivers.join(
            self.allocation,
            ['riderid', 'driverid']
        ).drop('sourceid') \
            .drop('destid') \
            .withColumn('pickup_lat', getLat(F.col('riderLocation'))) \
            .withColumn('pickup_lon', getLon(F.col('riderLocation'))) \
            .withColumn('driver_lat', getLat(F.col('driverLocation'))) \
            .withColumn('driver_lon', getLon(F.col('driverLocation'))) \
            .drop('riderLocation') \
            .drop('driverLocation')
        
        self.lastAllocationFile = self.db('rides/{}.csv'.format(now.strftime("%Y%d%m-%H%M%S-%f")))
        self.lastAllocation.write.csv(self.lastAllocationFile)
        self.lastAllocationList = self.lastAllocation \
            .select('pickup_lat', 'pickup_lon', 'driver_lat', 'driver_lon') \
            .toPandas().values.tolist()
        
        self.activeDrivers = self.activeDrivers \
            .join(self.allocation, ['driverid'], 'left_anti')

        self.updateDrivers()

        for i in self.allocation.select('driverid').collect():
            self.driversPosition.pop(i[0])

    def processDrivers(self, df, id):
        newDrivers = self.spark.createDataFrame(df.collect(), schema=self.driversStatusSchema)
        self.activeDrivers = self.activeDrivers.union(newDrivers)

    def updateCurrentRiders(self, df, id):
        newRiders = df.toPandas()
        newRiders.to_parquet(self.db('riders_request', '{}.parquet'.format(id)))

        self.currentRiders += newRiders.values[:, :-1].tolist()

        """ newRiders = self.spark.createDataFrame(newRiders, schema=self.ridersRequestSchema)
        self.currentRiders = self.currentRiders.union(newRiders) """

    def __init__(self, spark, originalDataPath, dbPath):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        self.spark = spark
        self.originalDataPath = originalDataPath
        self.dbPath = dbPath
        self.lastUpdate = datetime.datetime.utcnow()

        self.preprocessing()
        os.mkdir(self.db('riders_request'))
        os.mkdir(self.db('drivers_status'))

        self.zones = self.spark.sql('select movement_id, display_name, st_geomFromWKT(geometry) geometry from parquet.`data/base/rawZones.parquet`')
        self.zones.createOrReplaceTempView('zones')
        self.zones.cache()

        self.distances = self.spark.sql(
            'select * from parquet.`data/base/distances.parquet`'
        )
        self.distances.createOrReplaceTempView('distances')
        self.distances.cache()

        self.ridersRequestSchema = StructType([
            StructField("riderid", IntegerType(), True),
            StructField("lat", FloatType(), True),
            StructField("lon", FloatType(), True),
            StructField("requestTime", TimestampType(), True),
        ])

        # allocate requests        
        self.ridersRequest = self.spark.readStream \
            .schema(self.ridersRequestSchema) \
            .parquet(os.path.join(self.dbPath, 'riders_request')) \
            .writeStream \
            .foreachBatch(self.processBatch) \
            .outputMode('append') \
            .trigger(processingTime='2 minutes') \
            .start()

        # keep track of waiting riders
        self.currentRidersStream = self.spark.readStream \
            .format('kafka') \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "RIDER") \
            .load() \
            .select(F.from_json(F.col("value").cast("string"), self.ridersRequestSchema).alias("parsed_value")) \
            .select('parsed_value.*') \
            .writeStream \
            .foreachBatch(self.updateCurrentRiders) \
            .outputMode('append') \
            .trigger(processingTime='1 seconds') \
            .start()

        """ self.currentRiders = self.spark.createDataFrame([], self.ridersRequestSchema)
        self.currentRiders.cache() """
        self.currentRiders = []
        self.lastAllocationList = []
        self.currentDriversListCoord = []

        # drivers status
        self.driversStatusSchema = StructType([
            StructField("driverid", IntegerType(), True),
            StructField("requestTime", TimestampType(), True),
            StructField("active", BooleanType(), True),
        ])

        self.driversStatus = self.spark.readStream \
            .format('kafka') \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "DRIVER") \
            .load() \
            .select(F.from_json(F.col("value").cast("string"), self.driversStatusSchema).alias("parsed_value")) \
            .select('parsed_value.*') \
            .writeStream \
            .foreachBatch(self.processDrivers) \
            .outputMode('append') \
            .trigger(processingTime='1 seconds') \
            .start()
        
        self.activeDrivers = self.spark.createDataFrame([], self.driversStatusSchema)
        self.activeDrivers.cache()

        # used to simulate drivers position
        self.mobileUse = self.spark.sql(
            'select dayType, traversals, st_point(lon, lat) point from parquet.`data/base/rawMobileUse.parquet`'
        )
        self.driversPosition = {}