package org.menthal.model.events

import com.julianpeeters.avro.annotations._

@AvroTypeProvider("model/avro/app_install.avsc")
@AvroRecord
case class AppInstall(var id:Long, var userId:Long, var time:Long, var appName:String, var packageName:String)
