/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.locationtech.geomesa.raster.data

import java.io.Serializable
import java.util.{Map => JMap}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.Connector
import org.geotools.factory.Hints
import org.locationtech.geomesa.raster.feature.Raster
import org.locationtech.geomesa.raster.ingest.{IngestRasterParams, GeoserverClientService}

import scala.util.Try

trait CoverageStore {
  def getAuths(): String
  def getVisibility(): String
  def saveRaster(raster: Raster): Unit
  def registerToGeoserver(raster: Raster): Unit
}

/**
 *
 *  This class handles operations on a coverage, including cutting coverage into chunks
 *  and resembling chunks to a coverage, saving/retrieving coverage to/from data source,
 *  and registering coverage to Geoserver.
 *
 * @param rasterStore Raster store instance
 * @param geoserverClientServiceO Optional Geoserver client instance
 */
class AccumuloCoverageStore(val rasterStore: RasterStore,
                            val geoserverClientServiceO: Option[GeoserverClientService] = None)
  extends CoverageStore with Logging {

  Hints.putSystemDefault(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)

  rasterStore.ensureTableExists()

  def getAuths() = rasterStore.getAuths

  def getVisibility() = rasterStore.getVisibility

  def getRasters(rasterQuery: RasterQuery): Iterator[Raster] = rasterStore.getRasters(rasterQuery)

  def saveRaster(raster: Raster) = {
    rasterStore.putRaster(raster)
    registerToGeoserver(raster)
  }

  def registerToGeoserver(raster: Raster) {
    geoserverClientServiceO.foreach { geoserverClientService => {
      registerToGeoserver(raster, geoserverClientService)
      logger.debug(s"Register raster ${raster.id} to geoserver at ${geoserverClientService.geoserverUrl}")
    }}
  }

  private def registerToGeoserver(raster: Raster, geoserverClientService: GeoserverClientService) {
    geoserverClientService.registerRasterStyles()
    geoserverClientService.registerRaster(raster.id,
                                          raster.name,
                                          raster.time.getMillis,
                                          raster.id,
                                          "Raster data",
                                          raster.mbgh.hash,
                                          raster.mbgh.prec,
                                          None)
  }
}

object AccumuloCoverageStore extends Logging {
  import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory._
  import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.params._
  import org.locationtech.geomesa.raster.AccumuloStoreHelper

  def apply(config: JMap[String, Serializable]): AccumuloCoverageStore = {
    val visibility = AccumuloStoreHelper.getVisibility(config)
    val tableName = tableNameParam.lookUp(config).asInstanceOf[String]
    val useMock = java.lang.Boolean.valueOf(mockParam.lookUp(config).asInstanceOf[String])
    val connector =
      if (config.containsKey(connParam.key)) connParam.lookUp(config).asInstanceOf[Connector]
      else AccumuloStoreHelper.buildAccumuloConnector(config, useMock)
    val authorizationsProvider = AccumuloStoreHelper.getAuthorizationsProvider(config, connector)
    val collectStats = !useMock && Try(statsParam.lookUp(config).asInstanceOf[java.lang.Boolean] == true).getOrElse(false)

    val shardsConfig = shardsParam.lookupOpt(config)
    val writeMemoryConfig = writeMemoryParam.lookupOpt(config)
    val writeThreadsConfig = writeThreadsParam.lookupOpt(config)
    val queryThreadsConfig = queryThreadsParam.lookupOpt(config)

    val rasterOps =
      AccumuloBackedRasterOperations(connector,
                                     tableName,
                                     authorizationsProvider,
                                     visibility,
                                     shardsConfig,
                                     writeMemoryConfig,
                                     writeThreadsConfig,
                                     queryThreadsConfig,
                                     collectStats)

    val dsConnectConfig: Map[String, String] = Map(
      IngestRasterParams.ACCUMULO_INSTANCE -> instanceIdParam.lookUp(config).asInstanceOf[String],
      IngestRasterParams.ZOOKEEPERS -> zookeepersParam.lookUp(config).asInstanceOf[String],
      IngestRasterParams.ACCUMULO_USER -> userParam.lookUp(config).asInstanceOf[String],
      IngestRasterParams.ACCUMULO_PASSWORD -> passwordParam.lookUp(config).asInstanceOf[String],
      IngestRasterParams.TABLE -> tableName,
      IngestRasterParams.AUTHORIZATIONS -> authorizationsProvider.getAuthorizations.toString
    )

    val geoserverConfig = geoserverParam.lookUp(config).asInstanceOf[String]
    val geoserverClientServiceO: Option[GeoserverClientService] =
      if (geoserverConfig == null) None
      else {
        val gsConnectConfig: Map[String, String] =
          geoserverConfig.split(",").map(_.split("=") match {
            case Array(s1, s2) => (s1, s2)
            case _ =>
              logger.error("Failed to instantiate Geoserver client service: wrong parameters.")
              sys.exit()
          }).toMap
        Some(new GeoserverClientService(dsConnectConfig ++ gsConnectConfig))
      }

    new AccumuloCoverageStore(new RasterStore(rasterOps), geoserverClientServiceO)
  }

}
