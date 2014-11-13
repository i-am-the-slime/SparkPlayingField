package org.menthal.model.implicits

import org.joda.time.DateTime

/**
 * Created by konrad on 21.07.2014.
 */
object DateImplicits {
  implicit def dateToLong(dt:DateTime):Long = dt.getMillis
  implicit def longToDate(t: Long):DateTime = new DateTime(t)
}
