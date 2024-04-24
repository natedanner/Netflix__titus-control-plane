/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.common.util;

import java.util.Collection;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.netflix.titus.common.util.CollectionsExt.asSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class ProtobufCopyTest {

    private static Message outerValue;

    @BeforeClass
    public static void setUp() throws Exception {
        Message innerValue = ProtoMessageBuilder.newInner("innerValue1", "innerValue2");
        Message innerValue2 = ProtoMessageBuilder.newInner("inner2Value1", "inner2Value2");
        outerValue = ProtoMessageBuilder.newOuter(innerValue, 10, innerValue, innerValue2);
    }

    @Test
    public void testTopLevelFieldSelection() throws Exception {
        // Include all fields
        Message all = ProtobufExt.copy(outerValue, asSet("objectField", "primitiveField"));
        FieldDescriptor objectField = ProtoMessageBuilder.getAndAssertField(outerValue, "objectField");
        FieldDescriptor primitiveField = ProtoMessageBuilder.getAndAssertField(outerValue, "primitiveField");
        assertFieldHasValue(all, objectField);
        assertFieldHasValue(all, primitiveField);

        // Include only second field
        Message secondOnly = ProtobufExt.copy(outerValue, asSet("primitiveField"));
        assertFieldHasNoValue(secondOnly, objectField);
        assertFieldHasValue(secondOnly, primitiveField);
    }

    @Test
    public void testNestedSimpleFieldSelection() throws Exception {
        Message filtered = ProtobufExt.copy(outerValue, asSet("objectField.stringField1", "primitiveField"));
        FieldDescriptor objectField = ProtoMessageBuilder.getAndAssertField(outerValue, "objectField");
        FieldDescriptor primitiveField = ProtoMessageBuilder.getAndAssertField(outerValue, "primitiveField");
        FieldDescriptor objectArrayField = ProtoMessageBuilder.getAndAssertField(outerValue, "objectArrayField");
        FieldDescriptor stringField1 = ProtoMessageBuilder.getAndAssertField(objectField.getMessageType(), "stringField1");
        FieldDescriptor stringField2 = ProtoMessageBuilder.getAndAssertField(objectField.getMessageType(), "stringField2");

        assertFieldHasValue(filtered, objectField);
        assertFieldHasValue((Message) filtered.getField(objectField), stringField1);
        assertFieldHasNoValue((Message) filtered.getField(objectField), stringField2);

        assertFieldHasValue(filtered, primitiveField);
        assertFieldHasNoValue(filtered, objectArrayField);
    }

    @Test
    public void testCollectionFieldSelection() throws Exception {
        Message filtered = ProtobufExt.copy(outerValue, asSet("objectArrayField", "primitiveField"));
        FieldDescriptor objectField = outerValue.getDescriptorForType().findFieldByName("objectField");
        FieldDescriptor primitiveField = outerValue.getDescriptorForType().findFieldByName("primitiveField");
        FieldDescriptor objectArrayField = outerValue.getDescriptorForType().findFieldByName("objectArrayField");
        FieldDescriptor stringField1 = ProtoMessageBuilder.getAndAssertField(objectField.getMessageType(), "stringField1");
        FieldDescriptor stringField2 = ProtoMessageBuilder.getAndAssertField(objectField.getMessageType(), "stringField2");

        assertFieldHasNoValue(filtered, objectField);
        assertFieldHasValue(filtered, primitiveField);
        assertFieldHasValue(filtered, objectArrayField);

        Collection<Message> collection = (Collection<Message>) filtered.getField(objectArrayField);
        assertThat(collection).hasSize(2);
        for (Message inner : collection) {
            assertFieldHasValue(inner, stringField1);
            assertFieldHasValue(inner, stringField2);
        }
    }

    @Test
    public void testNestedCollectionFieldSelection() throws Exception {
        Message filtered = ProtobufExt.copy(outerValue, asSet("objectArrayField.stringField1", "primitiveField"));
        FieldDescriptor objectField = outerValue.getDescriptorForType().findFieldByName("objectField");
        FieldDescriptor objectArrayField = outerValue.getDescriptorForType().findFieldByName("objectArrayField");
        FieldDescriptor stringField1 = ProtoMessageBuilder.getAndAssertField(objectField.getMessageType(), "stringField1");
        FieldDescriptor stringField2 = ProtoMessageBuilder.getAndAssertField(objectField.getMessageType(), "stringField2");

        assertFieldHasNoValue(filtered, objectField);
        assertFieldHasValue(filtered, objectArrayField);

        Collection<Message> collection = (Collection<Message>) filtered.getField(objectArrayField);
        for (Message inner : collection) {
            assertFieldHasValue(inner, stringField1);
            assertFieldHasNoValue(inner, stringField2);
        }
    }

    private void assertFieldHasValue(Message entity, FieldDescriptor field) {
        Object value = entity.getField(field);
        assertThat(value).isNotNull();

        if (value instanceof DynamicMessage) {
            assertThat(((DynamicMessage) value).getAllFields()).isNotEmpty();
        } else if (value instanceof Collection) {
            assertThat((Collection) value).isNotEmpty();
        } else {
            assertThat(value).isNotNull();
        }
    }

    private void assertFieldHasNoValue(Message entity, FieldDescriptor field) {
        Object value = entity.getField(field);
        if (value != null) {
            if (value instanceof DynamicMessage) {
                assertThat(((DynamicMessage) value).getAllFields()).isEmpty();
            } else if (value instanceof String) {
                assertThat(value).isEqualTo("");
            } else if (value instanceof Collection) {
                assertThat((Collection) value).isEmpty();
            } else {
                fail("Expected null value for field " + field);
            }
        }
    }
}
