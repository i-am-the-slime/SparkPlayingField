package org.menthal.model.events

import com.julianpeeters.avro.annotations.AvroTypeProvider

@AvroTypeProvider("src/test/resources/AvroTypeProviderTest16.avro")
case class AvroTypeProviderTest16()

@AvroTypeProvider("src/test/resources/shitisasshitdoes.avro")
case class ShitIsAsShitDoes()
