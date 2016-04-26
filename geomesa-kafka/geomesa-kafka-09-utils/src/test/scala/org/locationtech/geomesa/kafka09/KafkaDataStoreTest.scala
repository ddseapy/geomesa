package org.locationtech.geomesa.kafka09
/***********************************************************************
  * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/


import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class KafkaDataStoreTest extends kafka.KafkaDataStoreTest

@RunWith(classOf[JUnitRunner])
class KafkaConsumerTest extends kafka.consumer.KafkaConsumerTest

@RunWith(classOf[JUnitRunner])
class OffsetManagerIntegrationTest extends kafka.consumer.offsets.OffsetManagerIntegrationTest
