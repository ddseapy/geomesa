package org.locationtech.geomesa.kafka09
/***********************************************************************
  * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/


import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka.AbstractKafkaDataStoreTest
import org.locationtech.geomesa.kafka.consumer.AbstractKafkaConsumerTest
import org.locationtech.geomesa.kafka.consumer.offsets.AbstractOffsetManagerIntegrationTest
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class KafkaDataStoreTest extends AbstractKafkaDataStoreTest

@RunWith(classOf[JUnitRunner])
class KafkaConsumerTest extends AbstractKafkaConsumerTest

@RunWith(classOf[JUnitRunner])
class OffsetManagerIntegrationTest extends AbstractOffsetManagerIntegrationTest