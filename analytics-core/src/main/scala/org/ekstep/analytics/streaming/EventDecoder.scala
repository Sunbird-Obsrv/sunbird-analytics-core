package org.ekstep.analytics.streaming

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import kafka.utils.VerifiableProperties
import kafka.serializer.Decoder

class EventDecoder[T](props: VerifiableProperties = null)(implicit mf:Manifest[T]) extends Decoder[T] {
    val encoding =
        if (props == null)
            "UTF8"
        else
            props.getString("serializer.encoding", "UTF8");
            
    def fromBytes(bytes: Array[Byte]): T = {
        JSONUtils.deserialize[T](new String(bytes, encoding));
    }
}