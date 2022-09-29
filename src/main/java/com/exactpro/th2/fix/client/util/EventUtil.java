/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.fix.client.util;

import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class EventUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventUtil.class);

    public static com.exactpro.th2.common.grpc.Event toEvent(String parentId, String name) {

        return toEvent(parentId, name, "Info", null);
    }

    public static com.exactpro.th2.common.grpc.Event toEvent(String parentId, String name, @Nullable Throwable cause) {

        return toEvent(parentId, name, "Error", cause);
    }

    public static com.exactpro.th2.common.grpc.Event toEvent(RawMessage message, String parentId, String name) {
        return toEvent(message, parentId, name, "Info", null);
    }

    public static com.exactpro.th2.common.grpc.Event toEvent(String parentId, String name, String type, @Nullable Throwable cause) {
        return toEvent(null, parentId, name, type, cause);
    }

    public static com.exactpro.th2.common.grpc.Event toEvent(RawMessage message, String parentId, String name, String type, @Nullable Throwable cause) {

        Event event = Event.start()
                .name(name)
                .type(type);

        if (message != null) {
            event.messageID(message
                    .getMetadata()
                    .getId());
        }

        if (cause != null) {
            event.exception(cause, true)
                    .status(Event.Status.FAILED);
        } else {
            event.status(Event.Status.PASSED);
        }

        com.exactpro.th2.common.grpc.Event result = null;
        try {
            result = event.toProto(EventID.newBuilder()
                    .setId(parentId)
                    .build());
        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to convert event to proto.", e);
        }
        return result;
    }

}