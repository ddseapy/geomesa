/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

object ErrorMode extends Enumeration {
  type ErrorMode = Value
  val SkipBadRecords: ErrorMode = Value("skip-bad-records")
  val RaiseErrors   : ErrorMode = Value("raise-errors")
  val Default       : ErrorMode = SkipBadRecords
}
